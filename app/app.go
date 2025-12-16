package app

import (
	"indexer/es"
	"indexer/store"
)

type App struct {
	st store.Store
	es *es.Client
}

func New(st store.Store, esClient *es.Client) *App {
	return &App{
		st: st,
		es: esClient,
	}
}
