package aggregation

import (
	"context"
)

type FetchParameters[Req any] struct {
	Request       Req
	NextPageToken any
}

type FetchResult[P any] struct {
	Items         []P
	NextPageToken any
}

func NewRootPlan[P any, Req any](fetcher func(FetchParameters[Req]) (FetchResult[P], error)) *RootPlan[P, Req] {
	return &RootPlan[P, Req]{fetcher: fetcher}
}

type RootPlan[P any, Req any] struct {
	fetcher func(FetchParameters[Req]) (FetchResult[P], error)
}

func (p *RootPlan[R, Req]) Execute(ctx context.Context, params Req) (<-chan ExecutionResult[R], error) {
	var npt any
	ch := make(chan ExecutionResult[R])
	go func() {
		defer close(ch)
		for {
			result, err := p.fetcher(FetchParameters[Req]{Request: params, NextPageToken: npt})
			if err != nil {
				ch <- ExecutionResult[R]{Err: err}
				return
			}

			ch <- ExecutionResult[R]{Items: result.Items}

			if result.NextPageToken == nil {
				return
			}
			npt = result.NextPageToken
		}
	}()
	return ch, nil
}

type SubFetcher[Parent any] interface {
	Fetch(Parent) (any, error)
}

func NewSubPlan[Parent any, Req any, Result any](
	root Executor[Parent, Req],
	fetcher SubFetcher[Parent],
	builder func(Parent, any) Result,
) *SubPlan[Parent, Result, Req] {
	return &SubPlan[Parent, Result, Req]{Parent: root, Fetcher: fetcher, Builder: builder}
}

type SubPlan[Parent any, Result any, Req any] struct {
	Parent  Executor[Parent, Req]
	Fetcher SubFetcher[Parent]
	Builder func(Parent, any) Result
}

func (p *SubPlan[P, R, Req]) Execute(ctx context.Context, rootParams Req) (<-chan ExecutionResult[R], error) {
	ch := make(chan ExecutionResult[R])
	go func() {
		defer close(ch)
		parentCh, err := p.Parent.Execute(ctx, rootParams)
		if err != nil {
			ch <- ExecutionResult[R]{Err: err}
			return
		}

		for parentItems := range parentCh {
			if parentItems.Err != nil {
				ch <- ExecutionResult[R]{Err: parentItems.Err}
				return
			}

			rowResult := make([]R, len(parentItems.Items))
			for i, parentItem := range parentItems.Items {
				fetchResult, err := p.Fetcher.Fetch(parentItem)
				if err != nil {
					ch <- ExecutionResult[R]{Err: err}
					return
				}

				rowResult[i] = p.Builder(parentItem, fetchResult)
			}

			ch <- ExecutionResult[R]{Items: rowResult}
		}
	}()
	return ch, nil
}

type ExecutionResult[P any] struct {
	Items []P
	Err   error
}

type Executor[P, Req any] interface {
	Execute(ctx context.Context, params Req) (<-chan ExecutionResult[P], error)
}
