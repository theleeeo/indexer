package app

import (
	"indexer/es"
	"indexer/resource"
	"indexer/store"
)

type App struct {
	st store.Store
	es *es.Client

	resources []*resource.Config
}

func New(st store.Store, esClient *es.Client, resources []*resource.Config) *App {
	return &App{
		st:        st,
		es:        esClient,
		resources: resources,
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
