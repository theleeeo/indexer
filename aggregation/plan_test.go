package aggregation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type MockFetcher[Parent any, Result any] struct {
	FetchFunc func(Parent) (any, error)
}

func (m *MockFetcher[Parent, Result]) Fetch(parent Parent) (any, error) {
	return m.FetchFunc(parent)
}

func Test_RootPlan_Execute(t *testing.T) {
	fetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		if params.NextPageToken == nil {
			return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: "token"}, nil
		}
		return FetchResult[string]{Items: []string{"c"}, NextPageToken: nil}, nil
	}

	rootPlan := NewRootPlan(fetcher)

	ch, err := rootPlan.Execute(context.Background(), "request")
	require.NoError(t, err)

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 2, len(results))
	require.Equal(t, []string{"a", "b"}, results[0].Items)
	require.Equal(t, []string{"c"}, results[1].Items)
}

func Test_SubPlan_Execute(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		if params.NextPageToken == nil {
			return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: "token"}, nil
		}
		return FetchResult[string]{Items: []string{"c"}, NextPageToken: nil}, nil
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			return parent + "_sub", nil
		},
	}

	builder := func(parent string, fetchResult any) string {
		return parent + "_" + fetchResult.(string)
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch, err := subPlan.Execute(context.Background(), "request")
	require.NoError(t, err)

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 2, len(results))
	require.Equal(t, []string{"a_a_sub", "b_b_sub"}, results[0].Items)
	require.Equal(t, []string{"c_c_sub"}, results[1].Items)
}

func Test_RootPlan_Execute_WithError(t *testing.T) {
	fetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: nil, NextPageToken: nil}, context.Canceled
	}

	rootPlan := NewRootPlan(fetcher)

	ch, err := rootPlan.Execute(context.Background(), "request")
	require.NoError(t, err)

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

func Test_SubPlan_Execute_WithError(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: nil}, nil
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			if parent == "a" {
				return nil, context.Canceled
			}
			return parent + "_sub", nil
		},
	}

	builder := func(parent string, fetchResult any) string {
		return parent + "_" + fetchResult.(string)
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch, err := subPlan.Execute(context.Background(), "request")
	require.NoError(t, err)

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

func Test_SubPlan_Execute_WithParentError(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: nil, NextPageToken: nil}, context.Canceled
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			return parent + "_sub", nil
		},
	}

	builder := func(parent string, fetchResult any) string {
		return parent + "_" + fetchResult.(string)
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch, err := subPlan.Execute(context.Background(), "request")
	require.NoError(t, err)

	var results []ExecutionResult[string]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.ErrorIs(t, results[0].Err, context.Canceled)
}

func Test_SubPlan_MultipleSubItems(t *testing.T) {
	rootFetcher := func(params FetchParameters[string]) (FetchResult[string], error) {
		return FetchResult[string]{Items: []string{"a", "b"}, NextPageToken: nil}, nil
	}

	subFetcher := &MockFetcher[string, string]{
		FetchFunc: func(parent string) (any, error) {
			return []string{parent + "_sub1", parent + "_sub2"}, nil
		},
	}

	type SubResult struct {
		Parent string
		Subs   []string
	}

	builder := func(parent string, fetchResult any) SubResult {
		return SubResult{
			Parent: parent,
			Subs:   fetchResult.([]string),
		}
	}

	rootPlan := NewRootPlan(rootFetcher)
	subPlan := NewSubPlan(rootPlan, subFetcher, builder)

	ch, err := subPlan.Execute(context.Background(), "request")
	require.NoError(t, err)

	var results []ExecutionResult[SubResult]
	for res := range ch {
		results = append(results, res)
	}

	require.Equal(t, 1, len(results))
	require.Equal(t, "a", results[0].Items[0].Parent)
	require.Equal(t, []string{"a_sub1", "a_sub2"}, results[0].Items[0].Subs)
	require.Equal(t, "b", results[0].Items[1].Parent)
	require.Equal(t, []string{"b_sub1", "b_sub2"}, results[0].Items[1].Subs)
}
