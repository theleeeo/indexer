package store

import (
	"github.com/theleeeo/indexer/model"
)

type Relation struct {
	Parent model.Resource
	Child  model.Resource
}
