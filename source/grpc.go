package source

import (
	"context"
	"fmt"

	pb "github.com/theleeeo/indexer/gen/provider/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCProvider implements Provider by calling a remote gRPC ProviderService.
type GRPCProvider struct {
	conn   *grpc.ClientConn
	client pb.ProviderServiceClient
}

// NewGRPCProvider dials the given address and returns a Provider backed by the
// remote ProviderService plugin.
func NewGRPCProvider(addr string) (*GRPCProvider, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial provider plugin %s: %w", addr, err)
	}
	return &GRPCProvider{
		conn:   conn,
		client: pb.NewProviderServiceClient(conn),
	}, nil
}

func (p *GRPCProvider) FetchResource(ctx context.Context, resourceType, resourceID string) (map[string]any, error) {
	resp, err := p.client.FetchResource(ctx, &pb.FetchResourceRequest{
		ResourceType: resourceType,
		ResourceId:   resourceID,
	})
	if err != nil {
		return nil, err
	}
	if resp.Data == nil {
		return nil, nil
	}
	return resp.Data.AsMap(), nil
}

func (p *GRPCProvider) FetchRelated(ctx context.Context, resourceType, key string) ([]map[string]any, error) {
	resp, err := p.client.FetchRelated(ctx, &pb.FetchRelatedRequest{
		ResourceType: resourceType,
		Key:          key,
	})
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, len(resp.Data))
	for i, s := range resp.Data {
		result[i] = s.AsMap()
	}
	return result, nil
}

// Close releases the underlying gRPC connection.
func (p *GRPCProvider) Close() error {
	return p.conn.Close()
}
