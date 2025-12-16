package server

import (
	"context"
	"fmt"

	"indexer/app"
	"indexer/gen/index/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	AIndex = "a_search"
	BIndex = "b_search"
	CIndex = "c_search"
)

type IndexerServer struct {
	index.UnimplementedIndexServiceServer

	app *app.App
}

func NewIndexer(app *app.App) *IndexerServer {
	return &IndexerServer{
		app: app,
	}
}

func (s *IndexerServer) Publish(ctx context.Context, req *index.PublishRequest) (*index.PublishResponse, error) {
	if req.Event == nil {
		return nil, status.Error(codes.InvalidArgument, "event is required")
	}

	if err := s.applyOne(ctx, req.Event); err != nil {
		return nil, err
	}
	return &index.PublishResponse{}, nil
}

func (s *IndexerServer) PublishBatch(ctx context.Context, req *index.PublishBatchRequest) (*index.PublishBatchResponse, error) {
	if len(req.Events) == 0 {
		return &index.PublishBatchResponse{}, nil
	}

	for _, ev := range req.Events {
		if ev == nil {
			continue
		}
		if err := s.applyOne(ctx, ev); err != nil {
			return nil, err
		}
	}

	return &index.PublishBatchResponse{}, nil
}

func (s *IndexerServer) applyOne(ctx context.Context, ev *index.ChangeEvent) error {
	switch p := ev.Payload.(type) {
	case *index.ChangeEvent_CreatePayload:
		return s.app.Create(ctx, p.CreatePayload)
	case *index.ChangeEvent_UpdatePayload:
		return s.app.Update(ctx, p.UpdatePayload)
	case *index.ChangeEvent_DeletePayload:
		return s.app.Delete(ctx, p.DeletePayload)
	case *index.ChangeEvent_SetRelationPayload:
		return s.app.SetRelation(ctx, p.SetRelationPayload)
	case *index.ChangeEvent_AddRelationPayload:
		return s.app.AddRelation(ctx, p.AddRelationPayload)
	case *index.ChangeEvent_RemoveRelationPayload:
		return s.app.RemoveRelation(ctx, p.RemoveRelationPayload)
	default:
		return fmt.Errorf("unknown payload")
	}
}
