package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"indexer/app"
	"indexer/gen/index/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		if errors.Is(err, app.ErrUnknownResource) {
			return nil, status.Error(codes.FailedPrecondition, "unknown resource")
		}

		var invalidArgsErr *app.InvalidArgumentError
		if errors.As(err, &invalidArgsErr) {
			return nil, status.Error(codes.InvalidArgument, invalidArgsErr.Msg)
		}
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

		// TODO: Apply in a transaction
		if err := s.applyOne(ctx, ev); err != nil {
			return nil, err
		}
	}

	return &index.PublishBatchResponse{}, nil
}

func (s *IndexerServer) applyOne(ctx context.Context, ev *index.ChangeEvent) error {
	switch p := ev.Payload.(type) {
	case *index.ChangeEvent_CreatePayload:
		return s.app.RegisterCreate(ctx, zeroTimeIfNil(ev.OccurredAt), p.CreatePayload)
	case *index.ChangeEvent_UpdatePayload:
		return s.app.RegisterUpdate(ctx, zeroTimeIfNil(ev.OccurredAt), p.UpdatePayload)
	case *index.ChangeEvent_DeletePayload:
		return s.app.RegisterDelete(ctx, zeroTimeIfNil(ev.OccurredAt), p.DeletePayload)
	case *index.ChangeEvent_SetRelationsPayload:
		return s.app.RegisterSetRelations(ctx, zeroTimeIfNil(ev.OccurredAt), p.SetRelationsPayload)
	case *index.ChangeEvent_AddRelationPayload:
		return s.app.RegisterAddRelation(ctx, zeroTimeIfNil(ev.OccurredAt), p.AddRelationPayload)
	case *index.ChangeEvent_RemoveRelationPayload:
		return s.app.RegisterRemoveRelation(ctx, zeroTimeIfNil(ev.OccurredAt), p.RemoveRelationPayload)
	default:
		return fmt.Errorf("unknown payload")
	}
}

func zeroTimeIfNil(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}
