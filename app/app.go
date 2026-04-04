package app

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

type App struct {
	st *store.PostgresStore
	es *es.Client

	queue *jobqueue.Queue

	resources resource.Configs

	builder *projection.Builder
}

func (a *App) SetResourceConfig(resources resource.Configs) {
	a.resources = resources
	if a.builder != nil {
		a.builder.SetResourceConfig(resources)
	}
}

func New(st *store.PostgresStore, esClient *es.Client, queue *jobqueue.Queue) *App {
	a := &App{
		st:    st,
		es:    esClient,
		queue: queue,
	}
	return a
}

// SetBuilder sets the projection builder. Must be called after SetResourceConfig.
func (a *App) SetBuilder(builder *projection.Builder) {
	a.builder = builder
}

func (a *App) verifyResourceConfig(resource, resourceId string) (*resource.Config, error) {
	if resource == "" {
		return nil, fmt.Errorf("resource required")
	}

	if resourceId == "" {
		return nil, fmt.Errorf("resource_id required")
	}

	r := a.resources.Get(resource)
	if r == nil {
		return nil, ErrUnknownResource
	}

	return r, nil
}
