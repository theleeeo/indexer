package core

import (
	"errors"
	"fmt"

	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/jobqueue"
	"github.com/theleeeo/indexer/projection"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/store"
)

var (
	ErrUnknownResource = errors.New("unknown resource")
)

type InvalidArgumentError struct {
	Msg string
}

func (e *InvalidArgumentError) Error() string {
	return e.Msg
}

// Config holds the dependencies required to create an Indexer.
type Config struct {
	// Builder is the projection builder that fetches and assembles documents.
	// Use projection.BuildPlansFromConfig to create plans from a resource config,
	// or provide custom plans for library usage.
	Builder *projection.Builder

	// Resources defines the resource types, fields, and relations.
	Resources resource.Configs

	// ES is the Elasticsearch client for indexing and searching.
	ES *es.Client

	// Store is the PostgreSQL relation-graph store.
	Store *store.PostgresStore

	// Queue is the job queue for enqueuing rebuild/delete jobs.
	Queue *jobqueue.Queue
}

// Indexer is the core indexing engine. It receives change notifications,
// determines which search documents are affected, rebuilds them from
// authoritative source data, and writes them to Elasticsearch.
type Indexer struct {
	st *store.PostgresStore
	es *es.Client

	queue *jobqueue.Queue

	resources resource.Configs

	builder *projection.Builder
}

// New creates a new Indexer with the given configuration.
func New(cfg Config) *Indexer {
	return &Indexer{
		st:        cfg.Store,
		es:        cfg.ES,
		queue:     cfg.Queue,
		resources: cfg.Resources,
		builder:   cfg.Builder,
	}
}

// SetBuilder replaces the projection builder and resource configuration.
// This is primarily used by the standalone application with YAML DSL;
// library users typically set these once at construction via Config.
func (idx *Indexer) SetBuilder(builder *projection.Builder, resources resource.Configs) {
	idx.resources = resources
	idx.builder = builder
}

func (idx *Indexer) verifyResourceConfig(resource, resourceId string) (*resource.Config, error) {
	if resource == "" {
		return nil, fmt.Errorf("resource required")
	}

	if resourceId == "" {
		return nil, fmt.Errorf("resource_id required")
	}

	r := idx.resources.Get(resource)
	if r == nil {
		return nil, ErrUnknownResource
	}

	return r, nil
}
