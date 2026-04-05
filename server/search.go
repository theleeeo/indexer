package server

import (
	"context"
	"errors"

	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/gen/search/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SearcherServer struct {
	search.UnimplementedSearchServiceServer

	idx *core.Indexer
}

func NewSearcher(idx *core.Indexer) *SearcherServer {
	return &SearcherServer{
		idx: idx,
	}
}

func (s *SearcherServer) Search(ctx context.Context, req *search.SearchRequest) (*search.SearchResponse, error) {
	resp, err := s.idx.Search(ctx, req)
	if err != nil {
		if errors.Is(err, core.ErrUnknownResource) {
			return nil, status.Error(codes.FailedPrecondition, core.ErrUnknownResource.Error())
		}
		return nil, err
	}
	return resp, err
}
