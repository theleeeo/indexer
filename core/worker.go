package core

import (
	"context"

	"github.com/riverqueue/river"
)

// River job args + workers.
//
// Each args type implements river.JobArgs via Kind(). Each worker embeds
// river.WorkerDefaults and holds a reference to the Indexer so it can
// dispatch to the existing handle* methods.

// RebuildArgs is the River job args for rebuilding a single root document.
type RebuildArgs struct {
	ResourceType string            `json:"resource_type"`
	ResourceID   string            `json:"resource_id"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func (RebuildArgs) Kind() string { return "rebuild" }

// DeleteArgs is the River job args for deleting a single root document.
type DeleteArgs struct {
	ResourceType string            `json:"resource_type"`
	ResourceID   string            `json:"resource_id"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

func (DeleteArgs) Kind() string { return "delete" }

// FullRebuildArgs is the River job args for a full rebuild of a resource type.
type FullRebuildArgs struct {
	Selector ResourceSelector  `json:"selector"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (FullRebuildArgs) Kind() string { return "full_rebuild" }

// RebuildWorker processes "rebuild" jobs.
type RebuildWorker struct {
	river.WorkerDefaults[RebuildArgs]
	Idx *Indexer
}

func (w *RebuildWorker) Work(ctx context.Context, job *river.Job[RebuildArgs]) error {
	return w.Idx.handleRebuild(ctx, RebuildPayload{
		ResourceType: job.Args.ResourceType,
		ResourceID:   job.Args.ResourceID,
		Metadata:     job.Args.Metadata,
	})
}

// DeleteWorker processes "delete" jobs.
type DeleteWorker struct {
	river.WorkerDefaults[DeleteArgs]
	Idx *Indexer
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	return w.Idx.handleDelete(ctx, RebuildPayload{
		ResourceType: job.Args.ResourceType,
		ResourceID:   job.Args.ResourceID,
		Metadata:     job.Args.Metadata,
	})
}

// FullRebuildWorker processes "full_rebuild" jobs.
type FullRebuildWorker struct {
	river.WorkerDefaults[FullRebuildArgs]
	Idx *Indexer
}

func (w *FullRebuildWorker) Work(ctx context.Context, job *river.Job[FullRebuildArgs]) error {
	return w.Idx.handleFullRebuild(ctx, FullRebuildPayload{
		Selector: job.Args.Selector,
		Metadata: job.Args.Metadata,
	})
}

// RegisterWorkers adds the indexer's River workers to the given registry.
// Callers should invoke this before constructing the River client.
func RegisterWorkers(workers *river.Workers, idx *Indexer) {
	river.AddWorker(workers, &RebuildWorker{Idx: idx})
	river.AddWorker(workers, &DeleteWorker{Idx: idx})
	river.AddWorker(workers, &FullRebuildWorker{Idx: idx})
}
