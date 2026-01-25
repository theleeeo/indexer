package store

import (
	"indexer/model"
)

type Relation struct {
	Parent model.Resource
	Child  model.Resource
}
