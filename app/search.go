package app

import (
	"context"
	"indexer/gen/search/v1"
)

func (a *App) Search(ctx context.Context, indexAlias string, req *search.SearchRequest, searchFields []string) (*search.SearchResponse, error) {
	if req.PageSize <= 0 {
		req.PageSize = 25
	}
	if req.PageSize > 100 {
		req.PageSize = 100
	}
	if req.Page < 0 {
		req.Page = 0
	}

	res, err := a.es.Search(ctx, indexAlias, req, searchFields)
	if err != nil {
		return nil, err
	}

	return res, nil
}
