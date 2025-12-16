package app

import (
	"context"
	"fmt"
	"indexer/gen/search/v1"

	"google.golang.org/protobuf/types/known/structpb"
)

func (a *App) Search(ctx context.Context, indexAlias string, req *search.SearchRequest, searchFields []string) (*search.SearchResponse, error) {
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

	res, err := a.es.Search(ctx, indexAlias, body)
	if err != nil {
		return nil, err
	}

	out := &search.SearchResponse{Total: res.Total}
	for _, h := range res.Hits {
		st, err := structpb.NewStruct(h.Source)
		if err != nil {
			// if struct conversion fails, skip rather than fail the whole query
			continue
		}
		out.Hits = append(out.Hits, &search.SearchHit{
			Id:     h.ID,
			Score:  h.Score,
			Source: st,
		})
	}
	return out, nil
}

func buildFilterClause(f *search.Filter) (any, error) {
	var inner any

	switch f.Op {
	case search.FilterOp_FILTER_OP_EQ:
		if f.Value == "" {
			return nil, fmt.Errorf("EQ filter requires value for field %q", f.Field)
		}
		inner = map[string]any{"term": map[string]any{f.Field: f.Value}}

	case search.FilterOp_FILTER_OP_IN:
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
