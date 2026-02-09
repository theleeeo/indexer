package store

import (
	"indexer/model"
)

type Relation struct {
	Parent model.Resource
	Child  model.Resource
	// Pending deletion flag.
	// This is to signal that the relation is in the process of being deleted, so that it can be ignored in queries that are executed concurrently with the deletion.
	// Deleted bool
}

// TODO: Store interface with an "As transaction" method that returns a transaction-scoped store
