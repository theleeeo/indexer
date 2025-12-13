package server

import (
	"context"
	"fmt"
	"time"

	"indexer/es"
	"indexer/gen/index/v1"
	"indexer/model"
	"indexer/store"
)

const (
	AIndex = "a_search"
	BIndex = "b_search"
	CIndex = "c_search"
)

type IndexerServer struct {
	index.UnimplementedIndexerServer

	st *store.Store
	es *es.Client

	// de-dup window (in-memory)
	dedupTTL time.Duration
}

func NewIndexer(st *store.Store, esClient *es.Client) *IndexerServer {
	return &IndexerServer{
		st:       st,
		es:       esClient,
		dedupTTL: 5 * time.Minute,
	}
}

func (s *IndexerServer) Publish(ctx context.Context, ev *index.ChangeEvent) (*index.PublishResponse, error) {
	if ev == nil {
		return &index.PublishResponse{Accepted: 0}, nil
	}

	if err := s.applyOne(ctx, ev); err != nil {
		return nil, err
	}
	return &index.PublishResponse{Accepted: 1}, nil
}

func (s *IndexerServer) PublishBatch(ctx context.Context, batch *index.ChangeBatch) (*index.PublishResponse, error) {
	if batch == nil || len(batch.Events) == 0 {
		return &index.PublishResponse{Accepted: 0}, nil
	}
	var accepted int64
	for _, ev := range batch.Events {
		if ev == nil {
			continue
		}
		if err := s.applyOne(ctx, ev); err != nil {
			return nil, err
		}
		accepted++
	}
	return &index.PublishResponse{Accepted: accepted}, nil
}

func (s *IndexerServer) applyOne(ctx context.Context, ev *index.ChangeEvent) error {
	switch p := ev.Payload.(type) {
	case *index.ChangeEvent_AUpsert:
		return s.handleAUpsert(ctx, p.AUpsert)
	case *index.ChangeEvent_ADelete:
		return s.handleADelete(ctx, p.ADelete)
	case *index.ChangeEvent_BUpsert:
		return s.handleBUpsert(ctx, p.BUpsert)
	case *index.ChangeEvent_BDelete:
		return s.handleBDelete(ctx, p.BDelete)
	case *index.ChangeEvent_CUpsert:
		return s.handleCUpsert(ctx, p.CUpsert)
	case *index.ChangeEvent_CDelete:
		return s.handleCDelete(ctx, p.CDelete)
	case *index.ChangeEvent_CreatePayload:
		return s.handleCreate(ctx, p.CreatePayload)
	default:
		return fmt.Errorf("unknown payload")
	}
}

func (s *IndexerServer) handleCreate(ctx context.Context, p *index.CreatePayload) error {
	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	relationsMap := make(map[string][]string)
	for _, rl := range p.Relations {
		relationsMap[rl.Resource] = append(relationsMap[rl.Resource], rl.ResourceId)
	}

	s.st.StoreResource(p.Resource, p.ResourceId, relationsMap)

	if err := s.es.UpsertJSON(ctx, p.Resource+"_search", p.ResourceId, map[string]any{
		"doc": p.Data,
	}); err != nil {
		return err
	}

	relatedResources := s.st.GetRelatedResources(p.Resource, p.ResourceId)
	for _, relatedResource := range relatedResources {
		rsType, rsId := store.KeyParts(relatedResource)
		if err := s.es.UpsertFieldElementByID(ctx, rsType+"_search", rsId, p.Resource, p.ResourceId, p.Data); err != nil {
			return err
		}
	}

	return nil
}

func (s *IndexerServer) handleUpdate(ctx context.Context, p *index.UpdatePayload) error {
	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	relationsToAdd := make(map[string][]string)
	relationsToRemove := make(map[string][]string)
	for _, rc := range p.RelationChanges {
		switch rc.ChangeType {
		case index.RelationChange_CHANGE_TYPE_ADDED:
			relationsToAdd[rc.Relation.Resource] = append(relationsToAdd[rc.Relation.Resource], rc.Relation.ResourceId)
		case index.RelationChange_CHANGE_TYPE_REMOVED:
			relationsToRemove[rc.Relation.Resource] = append(relationsToRemove[rc.Relation.Resource], rc.Relation.ResourceId)
		}
	}

	s.st.UpdateResource(p.Resource, p.ResourceId, relationsToAdd, relationsToRemove)

	return s.es.UpsertJSON(ctx, p.Resource+"_search", p.ResourceId, map[string]any{
		"doc": p.Data,
	})
}

func (s *IndexerServer) handleAUpsert(ctx context.Context, a *index.AUpsert) error {
	if a.AId == "" {
		return fmt.Errorf("a_id required")
	}

	bKeysToRefresh, cKeysToRefresh := s.st.UpsertA(a.AId, a.Status, a.BId, a.CIds)

	// upsert A doc
	if err := s.reindexAByKey(ctx, a.AId); err != nil {
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

func (s *IndexerServer) handleADelete(ctx context.Context, a *index.ADelete) error {
	if a.AId == "" {
		return fmt.Errorf("a_id required")
	}
	bKeysToRefresh, cKeysToRefresh := s.st.DeleteA(a.AId)

	// delete from a_search
	if err := s.es.Delete(ctx, AIndex, a.AId); err != nil {
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

func (s *IndexerServer) handleBUpsert(ctx context.Context, b *index.BUpsert) error {
	if b.BId == "" {
		return fmt.Errorf("b_id required")
	}
	s.st.UpsertB(b.BId, b.Name)

	// upsert B doc
	if err := s.reindexBByKey(ctx, b.BId); err != nil {
		return err
	}

	// fan-out: any A referencing this B needs reindex (because b.name changed)
	aKeys := s.st.AffectedAsByB(b.BId)
	return s.bulkReindexA(ctx, aKeys)
}

func (s *IndexerServer) handleBDelete(ctx context.Context, b *index.BDelete) error {
	if b.BId == "" {
		return fmt.Errorf("b_id required")
	}
	s.st.DeleteB(b.BId)

	// delete B doc
	if err := s.es.Delete(ctx, BIndex, b.BId); err != nil {
		return err
	}

	// fan-out: A docs referencing this B should drop B inline
	aKeys := s.st.AffectedAsByB(b.BId)
	return s.bulkReindexA(ctx, aKeys)
}

func (s *IndexerServer) handleCUpsert(ctx context.Context, c *index.CUpsert) error {
	if c.CId == "" {
		return fmt.Errorf("c_id required")
	}
	s.st.UpsertC(c.CId, c.Type, c.State)

	// upsert C doc
	if err := s.reindexCByKey(ctx, c.CId); err != nil {
		return err
	}

	// fan-out: any A that includes this C needs reindex (because c fields changed)
	aKeys := s.st.AffectedAsByC(c.CId)
	return s.bulkReindexA(ctx, aKeys)
}

func (s *IndexerServer) handleCDelete(ctx context.Context, c *index.CDelete) error {
	if c.CId == "" {
		return fmt.Errorf("c_id required")
	}
	s.st.DeleteC(c.CId)

	// delete C doc
	if err := s.es.Delete(ctx, CIndex, c.CId); err != nil {
		return err
	}

	// fan-out: A docs that had this C should drop it inline
	aKeys := s.st.AffectedAsByC(c.CId)
	return s.bulkReindexA(ctx, aKeys)
}

func (s *IndexerServer) reindexAByKey(ctx context.Context, aKey string) error {
	aProj := s.st.SnapshotA(aKey)
	if aProj == nil {
		return nil
	}

	var bProj *store.BProj
	if aProj.BID != "" {
		bProj = s.st.SnapshotB(aProj.BID)
	}

	cs := make([]*store.CProj, 0, len(aProj.CIDs))
	for cKey := range aProj.CIDs {
		cs = append(cs, s.st.SnapshotC(cKey))
	}

	doc := model.BuildADoc(aProj, bProj, cs)
	return s.es.UpsertJSON(ctx, AIndex, aKey, doc)
}

func (s *IndexerServer) reindexBByKey(ctx context.Context, bKey string) error {
	bProj := s.st.SnapshotB(bKey)
	if bProj == nil {
		return nil
	}
	aCount := s.st.CountAsForB(bKey)
	doc := model.BuildBDoc(bProj, aCount)
	return s.es.UpsertJSON(ctx, BIndex, bKey, doc)
}

func (s *IndexerServer) reindexCByKey(ctx context.Context, cKey string) error {
	cProj := s.st.SnapshotC(cKey)
	if cProj == nil {
		return nil
	}
	aCount := s.st.CountAsForC(cKey)
	doc := model.BuildCDoc(cProj, aCount)
	return s.es.UpsertJSON(ctx, CIndex, cKey, doc)
}

func (s *IndexerServer) bulkReindexA(ctx context.Context, aKeys []string) error {
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
			bProj = s.st.SnapshotB(aProj.BID)
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
