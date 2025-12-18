package server

import (
	"context"
	"errors"
	"indexer/app"
	"indexer/gen/search/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SearcherServer struct {
	search.UnimplementedSearchServiceServer

	app *app.App
}

func NewSearcher(app *app.App) *SearcherServer {
	return &SearcherServer{
		app: app,
	}
}

func (s *SearcherServer) Search(ctx context.Context, req *search.SearchRequest) (*search.SearchResponse, error) {
	resp, err := s.app.Search(ctx, req)
	if err != nil {
		if errors.Is(err, app.ErrUnknownResource) {
			return nil, status.Error(codes.FailedPrecondition, app.ErrUnknownResource.Error())
		}
		return nil, err
	}
	return resp, err
}
