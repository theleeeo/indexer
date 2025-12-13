package server

import (
	"context"
	"fmt"
	"time"

	"indexer/es"
	"indexer/gen/indexer/v1"
	"indexer/model"
	"indexer/store"

	"google.golang.org/protobuf/types/known/structpb"
)

const (
	AIndex = "a_search"
	BIndex = "b_search"
	CIndex = "c_search"
)

type GRPCServer struct {
	indexer.UnimplementedIndexerServer

	st *store.Store
	es *es.Client

	// de-dup window (in-memory)
	dedupTTL time.Duration
}

func New(st *store.Store, esClient *es.Client) *GRPCServer {
	return &GRPCServer{
		st:       st,
		es:       esClient,
		dedupTTL: 5 * time.Minute,
	}
}

func (s *GRPCServer) Publish(ctx context.Context, ev *indexer.ChangeEvent) (*indexer.PublishResponse, error) {
	if ev == nil {
		return &indexer.PublishResponse{Accepted: 0}, nil
	}
	if ev.EventId == "" || ev.TenantId == "" {
		return nil, fmt.Errorf("event_id and tenant_id are required")
	}

	// best-effort idempotency (caller should still retry on transient errors)
	if s.st.SeenRecently(ev.EventId, s.dedupTTL) {
		return &indexer.PublishResponse{Accepted: 1}, nil
	}

	if err := s.applyOne(ctx, ev); err != nil {
		return nil, err
	}
	return &indexer.PublishResponse{Accepted: 1}, nil
}

func (s *GRPCServer) PublishBatch(ctx context.Context, batch *indexer.ChangeBatch) (*indexer.PublishResponse, error) {
	if batch == nil || len(batch.Events) == 0 {
		return &indexer.PublishResponse{Accepted: 0}, nil
	}
	var accepted int64
	for _, ev := range batch.Events {
		if ev == nil || ev.EventId == "" || ev.TenantId == "" {
			continue
		}
		if s.st.SeenRecently(ev.EventId, s.dedupTTL) {
			accepted++
			continue
		}
		if err := s.applyOne(ctx, ev); err != nil {
			return nil, err
		}
		accepted++
	}
	return &indexer.PublishResponse{Accepted: accepted}, nil
}

func (s *GRPCServer) applyOne(ctx context.Context, ev *indexer.ChangeEvent) error {
	switch p := ev.Payload.(type) {
	case *indexer.ChangeEvent_AUpsert:
		return s.handleAUpsert(ctx, ev.TenantId, p.AUpsert)
	case *indexer.ChangeEvent_ADelete:
		return s.handleADelete(ctx, ev.TenantId, p.ADelete)
	case *indexer.ChangeEvent_BUpsert:
		return s.handleBUpsert(ctx, ev.TenantId, p.BUpsert)
	case *indexer.ChangeEvent_BDelete:
		return s.handleBDelete(ctx, ev.TenantId, p.BDelete)
	case *indexer.ChangeEvent_CUpsert:
		return s.handleCUpsert(ctx, ev.TenantId, p.CUpsert)
	case *indexer.ChangeEvent_CDelete:
		return s.handleCDelete(ctx, ev.TenantId, p.CDelete)
	default:
		return fmt.Errorf("unknown payload")
	}
}

func (s *GRPCServer) handleAUpsert(ctx context.Context, tenant string, a *indexer.AUpsert) error {
	if a.AId == "" {
		return fmt.Errorf("a_id required")
	}

	bKeysToRefresh, cKeysToRefresh := s.st.UpsertA(tenant, a.AId, a.Status, a.BId, a.CIds)

	// upsert A doc
	if err := s.reindexAByKey(ctx, store.Key(tenant, a.AId)); err != nil {
		return err
	}

	// refresh B/C docs whose derived fields changed (e.g., a_count)
	for _, bKey := range bKeysToRefresh {
		if err := s.reindexBByKey(ctx, bKey); err != nil {
			return err
		}
	}
	for _, cKey := range cKeysToRefresh {
		if err := s.reindexCByKey(ctx, cKey); err != nil {
			return err
		}
	}
	return nil
}

func (s *GRPCServer) handleADelete(ctx context.Context, tenant string, a *indexer.ADelete) error {
	if a.AId == "" {
		return fmt.Errorf("a_id required")
	}
	bKeysToRefresh, cKeysToRefresh := s.st.DeleteA(tenant, a.AId)

	// delete from a_search
	if err := s.es.Delete(ctx, AIndex, store.Key(tenant, a.AId)); err != nil {
		return err
	}

	for _, bKey := range bKeysToRefresh {
		if err := s.reindexBByKey(ctx, bKey); err != nil {
			return err
		}
	}
	for _, cKey := range cKeysToRefresh {
		if err := s.reindexCByKey(ctx, cKey); err != nil {
			return err
		}
	}
	return nil
}

func (s *GRPCServer) handleBUpsert(ctx context.Context, tenant string, b *indexer.BUpsert) error {
	if b.BId == "" {
		return fmt.Errorf("b_id required")
	}
	s.st.UpsertB(tenant, b.BId, b.Name)

	// upsert B doc
	if err := s.reindexBByKey(ctx, store.Key(tenant, b.BId)); err != nil {
		return err
	}

	// fan-out: any A referencing this B needs reindex (because b.name changed)
	aKeys := s.st.AffectedAsByB(tenant, b.BId)
	return s.bulkReindexA(ctx, aKeys)
}

func (s *GRPCServer) handleBDelete(ctx context.Context, tenant string, b *indexer.BDelete) error {
	if b.BId == "" {
		return fmt.Errorf("b_id required")
	}
	s.st.DeleteB(tenant, b.BId)

	// delete B doc
	if err := s.es.Delete(ctx, BIndex, store.Key(tenant, b.BId)); err != nil {
		return err
	}

	// fan-out: A docs referencing this B should drop B inline
	aKeys := s.st.AffectedAsByB(tenant, b.BId)
	return s.bulkReindexA(ctx, aKeys)
}

func (s *GRPCServer) handleCUpsert(ctx context.Context, tenant string, c *indexer.CUpsert) error {
	if c.CId == "" {
		return fmt.Errorf("c_id required")
	}
	s.st.UpsertC(tenant, c.CId, c.Type, c.State)

	// upsert C doc
	if err := s.reindexCByKey(ctx, store.Key(tenant, c.CId)); err != nil {
		return err
	}

	// fan-out: any A that includes this C needs reindex (because c fields changed)
	aKeys := s.st.AffectedAsByC(tenant, c.CId)
	return s.bulkReindexA(ctx, aKeys)
}

func (s *GRPCServer) handleCDelete(ctx context.Context, tenant string, c *indexer.CDelete) error {
	if c.CId == "" {
		return fmt.Errorf("c_id required")
	}
	s.st.DeleteC(tenant, c.CId)

	// delete C doc
	if err := s.es.Delete(ctx, CIndex, store.Key(tenant, c.CId)); err != nil {
		return err
	}

	// fan-out: A docs that had this C should drop it inline
	aKeys := s.st.AffectedAsByC(tenant, c.CId)
	return s.bulkReindexA(ctx, aKeys)
}

func (s *GRPCServer) reindexAByKey(ctx context.Context, aKey string) error {
	aProj := s.st.SnapshotA(aKey)
	if aProj == nil {
		return nil
	}

	var bProj *store.BProj
	if aProj.BID != "" {
		bProj = s.st.SnapshotB(store.Key(aProj.TenantID, aProj.BID))
	}

	cs := make([]*store.CProj, 0, len(aProj.CIDs))
	for cKey := range aProj.CIDs {
		cs = append(cs, s.st.SnapshotC(cKey))
	}

	doc := model.BuildADoc(aProj, bProj, cs)
	return s.es.UpsertJSON(ctx, AIndex, aKey, doc)
}

func (s *GRPCServer) reindexBByKey(ctx context.Context, bKey string) error {
	bProj := s.st.SnapshotB(bKey)
	if bProj == nil {
		return nil
	}
	aCount := s.st.CountAsForB(bKey)
	doc := model.BuildBDoc(bProj, aCount)
	return s.es.UpsertJSON(ctx, BIndex, bKey, doc)
}

func (s *GRPCServer) reindexCByKey(ctx context.Context, cKey string) error {
	cProj := s.st.SnapshotC(cKey)
	if cProj == nil {
		return nil
	}
	aCount := s.st.CountAsForC(cKey)
	doc := model.BuildCDoc(cProj, aCount)
	return s.es.UpsertJSON(ctx, CIndex, cKey, doc)
}

func (s *GRPCServer) bulkReindexA(ctx context.Context, aKeys []string) error {
	if len(aKeys) == 0 {
		return nil
	}
	items := make([]es.BulkItem, 0, len(aKeys))

	for _, aKey := range aKeys {
		aProj := s.st.SnapshotA(aKey)
		if aProj == nil {
			continue
		}

		var bProj *store.BProj
		if aProj.BID != "" {
			bProj = s.st.SnapshotB(store.Key(aProj.TenantID, aProj.BID))
		}

		cs := make([]*store.CProj, 0, len(aProj.CIDs))
		for cKey := range aProj.CIDs {
			cs = append(cs, s.st.SnapshotC(cKey))
		}

		doc := model.BuildADoc(aProj, bProj, cs)
		items = append(items, es.BulkItem{Index: AIndex, ID: aKey, Doc: doc})
	}

	return s.es.BulkUpsert(ctx, items)
}

func (s *GRPCServer) SearchA(ctx context.Context, req *indexer.SearchRequest) (*indexer.SearchResponse, error) {
	return s.search(ctx, AIndex, req, defaultSearchFieldsA())
}

func (s *GRPCServer) SearchB(ctx context.Context, req *indexer.SearchRequest) (*indexer.SearchResponse, error) {
	return s.search(ctx, BIndex, req, defaultSearchFieldsB())
}

func (s *GRPCServer) SearchC(ctx context.Context, req *indexer.SearchRequest) (*indexer.SearchResponse, error) {
	return s.search(ctx, CIndex, req, defaultSearchFieldsC())
}

func (s *GRPCServer) search(ctx context.Context, indexAlias string, req *indexer.SearchRequest, searchFields []string) (*indexer.SearchResponse, error) {
	if req == nil || req.TenantId == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}

	pageSize := int(req.PageSize)
	if pageSize <= 0 {
		pageSize = 25
	}
	if pageSize > 100 {
		pageSize = 100
	}
	page := int(req.Page)
	if page < 0 {
		page = 0
	}

	boolQ := map[string]any{
		"must":   []any{},
		"filter": []any{},
	}

	// Always tenant filter
	boolQ["filter"] = append(boolQ["filter"].([]any), map[string]any{
		"term": map[string]any{"tenant_id": req.TenantId},
	})

	// Full-text query (optional)
	if req.Query != "" {
		boolQ["must"] = append(boolQ["must"].([]any), map[string]any{
			"multi_match": map[string]any{
				"query":  req.Query,
				"fields": searchFields,
			},
		})
	}

	// Structured filters
	for _, f := range req.Filters {
		if f == nil || f.Field == "" {
			continue
		}
		filterClause, err := buildFilterClause(f)
		if err != nil {
			return nil, err
		}
		boolQ["filter"] = append(boolQ["filter"].([]any), filterClause)
	}

	body := map[string]any{
		"query": map[string]any{"bool": boolQ},
		"from":  page * pageSize,
		"size":  pageSize,
	}

	// Sort (optional). If none provided, ES default scoring applies.
	if len(req.Sort) > 0 {
		var sorts []any
		for _, srt := range req.Sort {
			if srt == nil || srt.Field == "" {
				continue
			}
			order := "asc"
			if srt.Desc {
				order = "desc"
			}
			sorts = append(sorts, map[string]any{
				srt.Field: map[string]any{"order": order},
			})
		}
		if len(sorts) > 0 {
			body["sort"] = sorts
		}
	}

	res, err := s.es.Search(ctx, indexAlias, body)
	if err != nil {
		return nil, err
	}

	out := &indexer.SearchResponse{Total: res.Total}
	for _, h := range res.Hits {
		st, err := structpb.NewStruct(h.Source)
		if err != nil {
			// if struct conversion fails, skip rather than fail the whole query
			continue
		}
		out.Hits = append(out.Hits, &indexer.SearchHit{
			Id:     h.ID,
			Score:  h.Score,
			Source: st,
		})
	}
	return out, nil
}

func buildFilterClause(f *indexer.Filter) (any, error) {
	var inner any

	switch f.Op {
	case indexer.FilterOp_FILTER_OP_EQ:
		if f.Value == "" {
			return nil, fmt.Errorf("EQ filter requires value for field %q", f.Field)
		}
		inner = map[string]any{"term": map[string]any{f.Field: f.Value}}

	case indexer.FilterOp_FILTER_OP_IN:
		if len(f.Values) == 0 {
			return nil, fmt.Errorf("IN filter requires values for field %q", f.Field)
		}
		inner = map[string]any{"terms": map[string]any{f.Field: f.Values}}

	default:
		return nil, fmt.Errorf("unsupported filter op for field %q", f.Field)
	}

	// Nested wrapping (optional)
	if f.NestedPath != "" {
		return map[string]any{
			"nested": map[string]any{
				"path":  f.NestedPath,
				"query": inner,
			},
		}, nil
	}

	return inner, nil
}

func defaultSearchFieldsA() []string {
	// Adjust to what you actually index
	return []string{
		"a_id",
		"a_status",
		"b.name",
		"c.type",
		"c.state",
	}
}

func defaultSearchFieldsB() []string {
	return []string{
		"b_id",
		"b_name",
	}
}

func defaultSearchFieldsC() []string {
	return []string{
		"c_id",
		"c_type",
		"c_state",
	}
}
