package server

import (
	"context"
	"errors"

	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/gen/index/v1"
	"github.com/theleeeo/indexer/source"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type IndexerServer struct {
	index.UnimplementedIndexServiceServer

	idx *core.Indexer
}

func NewIndexer(idx *core.Indexer) *IndexerServer {
	return &IndexerServer{
		idx: idx,
	}
}

func (s *IndexerServer) NotifyChange(ctx context.Context, req *index.NotifyChangeRequest) (*index.NotifyChangeResponse, error) {
	if req.Notification == nil {
		return nil, status.Error(codes.InvalidArgument, "notification is required")
	}

	n := protoToNotification(req.Notification)

	if err := s.idx.RegisterChange(ctx, n); err != nil {
		return nil, mapAppError(err)
	}

	return &index.NotifyChangeResponse{}, nil
}

func (s *IndexerServer) NotifyChangeBatch(ctx context.Context, req *index.NotifyChangeBatchRequest) (*index.NotifyChangeBatchResponse, error) {
	if len(req.Notifications) == 0 {
		return &index.NotifyChangeBatchResponse{}, nil
	}

	for _, pn := range req.Notifications {
		if pn == nil {
			continue
		}
		n := protoToNotification(pn)
		if err := s.idx.RegisterChange(ctx, n); err != nil {
			return nil, mapAppError(err)
		}
	}

	return &index.NotifyChangeBatchResponse{}, nil
}

func protoToNotification(pn *index.ChangeNotification) source.Notification {
	n := source.Notification{
		ResourceType: pn.ResourceType,
		ResourceID:   pn.ResourceId,
	}

	switch pn.Kind {
	case index.ChangeKind_CHANGE_KIND_CREATED:
		n.Kind = source.ChangeCreated
	case index.ChangeKind_CHANGE_KIND_UPDATED:
		n.Kind = source.ChangeUpdated
	case index.ChangeKind_CHANGE_KIND_DELETED:
		n.Kind = source.ChangeDeleted
	}

	return n
}

func mapAppError(err error) error {
	if errors.Is(err, core.ErrUnknownResource) {
		return status.Error(codes.InvalidArgument, "unknown resource")
	}
	var invalidArgsErr *core.InvalidArgumentError
	if errors.As(err, &invalidArgsErr) {
		return status.Error(codes.InvalidArgument, invalidArgsErr.Msg)
	}
	return status.Error(codes.Internal, err.Error())
}
