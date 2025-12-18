package app

import (
	"context"
	"errors"
	"indexer/gen/search/v1"
)

var (
	ErrUnknownResource = errors.New("unknown resource")
)

func (a *App) Search(ctx context.Context, req *search.SearchRequest) (*search.SearchResponse, error) {
	if req.Resource == "" {
		return nil, errors.New("resource is required")
	}

	r := a.resolveResourceConfig(req.Resource)
	if r == nil {
		return nil, ErrUnknownResource
	}

	if req.PageSize <= 0 {
		req.PageSize = 25
	}
	if req.PageSize > 100 {
		req.PageSize = 100
	}
	if req.Page < 0 {
		req.Page = 0
	}

	res, err := a.es.Search(ctx, req, r.IndexName, r.GetSearchableFields())
	if err != nil {
		return nil, err
	}

	return res, nil
}
