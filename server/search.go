package server

import (
	"context"
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
	switch req.Index {
	case "":
		return nil, status.Error(codes.InvalidArgument, "index is required")
	case "a":
		return s.app.Search(ctx, AIndex, req, defaultSearchFieldsA())
	case "b":
		return s.app.Search(ctx, BIndex, req, defaultSearchFieldsB())
	case "c":
		return s.app.Search(ctx, CIndex, req, defaultSearchFieldsC())
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "unknown index %q", req.Index)
	}
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
