package server

import (
	"context"
	"fmt"
	"indexer/es"
	"indexer/gen/searcher"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type SearcherServer struct {
	searcher.UnimplementedSearchServiceServer

	es *es.Client
}

func NewSearcher(esClient *es.Client) *SearcherServer {
	return &SearcherServer{
		es: esClient,
	}
}

func (s *SearcherServer) Search(ctx context.Context, req *searcher.SearchRequest) (*searcher.SearchResponse, error) {
	switch req.Index {
	case "":
		return nil, status.Error(codes.InvalidArgument, "index is required")
	case "a":
		return s.search(ctx, AIndex, req, defaultSearchFieldsA())
	case "b":
		return s.search(ctx, BIndex, req, defaultSearchFieldsB())
	case "c":
		return s.search(ctx, CIndex, req, defaultSearchFieldsC())
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "unknown index %q", req.Index)
	}
}

func (s *SearcherServer) search(ctx context.Context, indexAlias string, req *searcher.SearchRequest, searchFields []string) (*searcher.SearchResponse, error) {
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
	// boolQ["filter"] = append(boolQ["filter"].([]any), map[string]any{
	// 	"term": map[string]any{"tenant_id": req.TenantId},
	// })

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

	out := &searcher.SearchResponse{Total: res.Total}
	for _, h := range res.Hits {
		st, err := structpb.NewStruct(h.Source)
		if err != nil {
			// if struct conversion fails, skip rather than fail the whole query
			continue
		}
		out.Hits = append(out.Hits, &searcher.SearchHit{
			Id:     h.ID,
			Score:  h.Score,
			Source: st,
		})
	}
	return out, nil
}

func buildFilterClause(f *searcher.Filter) (any, error) {
	var inner any

	switch f.Op {
	case searcher.FilterOp_FILTER_OP_EQ:
		if f.Value == "" {
			return nil, fmt.Errorf("EQ filter requires value for field %q", f.Field)
		}
		inner = map[string]any{"term": map[string]any{f.Field: f.Value}}

	case searcher.FilterOp_FILTER_OP_IN:
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
