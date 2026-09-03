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

type ExecutionResult[P any] struct {
	Items []P
	Err   error
	// NextPageToken is the token that fetches the page after this one, as the
	// root fetch reported it; nil on the final page. Stages map pages 1:1 and
	// forward it unchanged, so a consumer can checkpoint a paginated walk at
	// page granularity (core's resumable rebuild cursor).
	NextPageToken any
}

// Executer is the contract a plan is consumed through: a caller that only runs
// a plan depends on this, never on Plan. That is what lets a plan come from
// somewhere other than this package's builder — a hand-rolled stage is equally
// valid, which is how core's tests supply fixed documents without a fetcher.
//
// It cannot carry Sub and Map itself, because interface methods may not declare
// type parameters; that restriction is why those extension methods live on the
// concrete Plan below.
type Executer[Req, P any] interface {
	Execute(ctx context.Context, params Req) <-chan ExecutionResult[P]
}

// SubFetcher resolves a relation for one parent item. Fetched names the type
// the fetch produces, so the build stage receives it typed rather than having
// to assert on an any.
type SubFetcher[Parent, Fetched any] interface {
	Fetch(ctx context.Context, parent Parent) (Fetched, error)
}

// SubFetcherFunc adapts a plain function to SubFetcher, for relation fetches
// that need no state beyond what they close over.
type SubFetcherFunc[Parent, Fetched any] func(ctx context.Context, parent Parent) (Fetched, error)

func (f SubFetcherFunc[Parent, Fetched]) Fetch(ctx context.Context, parent Parent) (Fetched, error) {
	return f(ctx, parent)
}

// send delivers res on ch, honoring cancellation: when ctx is cancelled and no
// receiver is ready, it gives up instead of blocking forever on an abandoned
// channel. A receiver that is already parked on the channel is always served,
// so terminal errors reach consumers that keep draining. Reports whether the
// result was delivered.
func send[P any](ctx context.Context, ch chan<- ExecutionResult[P], res ExecutionResult[P]) bool {
	select {
	case ch <- res:
		return true
	default:
	}
	select {
	case ch <- res:
		return true
	case <-ctx.Done():
		return false
	}
}

// Plan is a chain of stages that produces P from Req. Build one with Root, then
// extend it by calling Sub and Map on it; each returns a new Plan and leaves the
// receiver untouched, so a plan can be branched by extending it twice.
//
// The zero Plan is not usable; it must come from Root.
type Plan[Req, P any] struct {
	stage Executer[Req, P]
}

// Root starts a plan from a paginated fetcher. The fetcher is called until it
// returns a nil NextPageToken, and every page is delivered as its own result.
func Root[Req, P any](fetch func(ctx context.Context, params FetchParameters[Req]) (FetchResult[P], error)) Plan[Req, P] {
	return Plan[Req, P]{stage: rootStage[Req, P]{fetch: fetch}}
}

func (p Plan[Req, P]) Execute(ctx context.Context, params Req) <-chan ExecutionResult[P] {
	return p.stage.Execute(ctx, params)
}

// Sub extends the plan with a relation fetch: for every item the current chain
// emits, fetcher resolves the related data and build combines the two.
//
// F and Q are the method's own type parameters, so the returned Plan's element
// type need not match the receiver's — this is what lets a chain change shape
// as it denormalizes.
func (p Plan[Req, P]) Sub[F, Q any](fetcher SubFetcher[P, F], build func(P, F) Q) Plan[Req, Q] {
	return Plan[Req, Q]{stage: subStage[Req, P, F, Q]{
		parent:  p.stage,
		fetcher: fetcher,
		build:   build,
	}}
}

// Map extends the plan with a pure transform. It fetches nothing; it exists for
// shaping stages that derive fields once the relations they need are resolved
// (for example the standardized search surfaces). An error from an earlier stage
// is forwarded without calling f.
func (p Plan[Req, P]) Map[Q any](f func(P) Q) Plan[Req, Q] {
	return Plan[Req, Q]{stage: mapStage[Req, P, Q]{parent: p.stage, f: f}}
}

type rootStage[Req, P any] struct {
	fetch func(ctx context.Context, params FetchParameters[Req]) (FetchResult[P], error)
}

func (s rootStage[Req, P]) Execute(ctx context.Context, params Req) <-chan ExecutionResult[P] {
	var npt any
	ch := make(chan ExecutionResult[P])
	go func() {
		defer close(ch)
		for {
			if err := ctx.Err(); err != nil {
				send(ctx, ch, ExecutionResult[P]{Err: err})
				return
			}

			result, err := s.fetch(ctx, FetchParameters[Req]{Request: params, NextPageToken: npt})
			if err != nil {
				send(ctx, ch, ExecutionResult[P]{Err: err})
				return
			}

			if !send(ctx, ch, ExecutionResult[P]{Items: result.Items, NextPageToken: result.NextPageToken}) {
				return
			}

			if result.NextPageToken == nil {
				return
			}
			npt = result.NextPageToken
		}
	}()
	return ch
}

type subStage[Req, P, F, Q any] struct {
	parent  Executer[Req, P]
	fetcher SubFetcher[P, F]
	build   func(P, F) Q
}

func (s subStage[Req, P, F, Q]) Execute(ctx context.Context, params Req) <-chan ExecutionResult[Q] {
	ch := make(chan ExecutionResult[Q])
	go func() {
		defer close(ch)

		for parentItems := range s.parent.Execute(ctx, params) {
			if parentItems.Err != nil {
				send(ctx, ch, ExecutionResult[Q]{Err: parentItems.Err})
				return
			}

			rowResult := make([]Q, len(parentItems.Items))
			for i, parentItem := range parentItems.Items {
				if err := ctx.Err(); err != nil {
					send(ctx, ch, ExecutionResult[Q]{Err: err})
					return
				}

				fetched, err := s.fetcher.Fetch(ctx, parentItem)
				if err != nil {
					send(ctx, ch, ExecutionResult[Q]{Err: err})
					return
				}

				rowResult[i] = s.build(parentItem, fetched)
			}

			if !send(ctx, ch, ExecutionResult[Q]{Items: rowResult, NextPageToken: parentItems.NextPageToken}) {
				return
			}
		}
	}()
	return ch
}

type mapStage[Req, P, Q any] struct {
	parent Executer[Req, P]
	f      func(P) Q
}

func (s mapStage[Req, P, Q]) Execute(ctx context.Context, params Req) <-chan ExecutionResult[Q] {
	ch := make(chan ExecutionResult[Q])
	go func() {
		defer close(ch)
		for res := range s.parent.Execute(ctx, params) {
			if res.Err != nil {
				send(ctx, ch, ExecutionResult[Q]{Err: res.Err})
				return
			}
			// A fresh slice rather than an in-place rewrite: P and Q may differ,
			// and the parent stage's slice is not ours to mutate.
			items := make([]Q, len(res.Items))
			for i := range res.Items {
				items[i] = s.f(res.Items[i])
			}
			if !send(ctx, ch, ExecutionResult[Q]{Items: items, NextPageToken: res.NextPageToken}) {
				return
			}
		}
	}()
	return ch
}
