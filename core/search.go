package core

import (
	"context"
	"errors"

	"github.com/theleeeo/indexer/gen/search/v1"
)

// Search executes a search query against the Elasticsearch index for the given resource.
func (idx *Indexer) Search(ctx context.Context, req *search.SearchRequest) (*search.SearchResponse, error) {
	if req.Resource == "" {
		return nil, errors.New("resource is required")
	}

	r := idx.resources.Get(req.Resource)
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

	res, err := idx.es.Search(ctx, req, r.Resource+"_search", r.GetSearchableFields())
	if err != nil {
		return nil, err
	}

	return res, nil
}
