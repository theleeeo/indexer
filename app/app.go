package app

import (
	"errors"
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
	st store.Store
	es *es.Client

	queue *jobqueue.Queue

	resources []*resource.Config
}

func New(st store.Store, esClient *es.Client, resources []*resource.Config, queue *jobqueue.Queue) *App {
	return &App{
		st:        st,
		es:        esClient,
		resources: resources,
		queue:     queue,
	}
}

func (a *App) resolveResourceConfig(resourceName string) *resource.Config {
	for _, rc := range a.resources {
		if rc.Resource == resourceName {
			return rc
		}
	}
	return nil
}
