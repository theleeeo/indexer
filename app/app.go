package app

import (
	"errors"
	"fmt"
	"indexer/es"
	"indexer/jobqueue"
	"indexer/resource"
	"indexer/store"
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
}

func (a *App) SetResourceConfig(resources resource.Configs) {
	a.resources = resources
}

func New(st *store.PostgresStore, esClient *es.Client, queue *jobqueue.Queue) *App {
	return &App{
		st:    st,
		es:    esClient,
		queue: queue,
	}
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
