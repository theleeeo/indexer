package store

import (
	"errors"

	"github.com/theleeeo/indexer/model"
)

// ErrStaleVersion is returned when a resource upsert is rejected because the
// provided version is not strictly greater than the currently stored version.
var ErrStaleVersion = errors.New("stale version")

type Relation struct {
	Parent model.Resource
	Child  model.Resource
}
