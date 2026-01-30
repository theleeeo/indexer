package store

import (
	"indexer/model"
)

type Relation struct {
	Parent model.Resource
	Child  model.Resource
}

// TODO: Store interface with an "As transaction" method that returns a transaction-scoped store
