package core

import (
	"errors"
	"fmt"

	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/jobqueue"
	"github.com/theleeeo/indexer/projection"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/source"
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
	// Provider fetches authoritative data from the source service.
	Provider source.Provider

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
// The projection builder is created automatically from the provided
// Provider, Resources, and Store.
func New(cfg Config) *Indexer {
	builder := projection.NewBuilder(cfg.Provider, cfg.Resources, cfg.Store)

	return &Indexer{
		st:        cfg.Store,
		es:        cfg.ES,
		queue:     cfg.Queue,
		resources: cfg.Resources,
		builder:   builder,
	}
}

// SetResourceConfig dynamically updates the resource configuration.
// This is primarily used by the standalone application with YAML DSL;
// library users typically set the config once at construction via Config.
func (idx *Indexer) SetResourceConfig(resources resource.Configs) {
	idx.resources = resources
	idx.builder.SetResourceConfig(resources)
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
