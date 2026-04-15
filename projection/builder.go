package projection

import (
	"github.com/theleeeo/indexer/aggregation"
	"github.com/theleeeo/indexer/model"
)

// BuildRequest is the request parameter for the aggregation plan.
type BuildRequest struct {
	ResourceType string
	ResourceID   string
}

// BuildDoc is the intermediate document flowing through the aggregation plan.
// It carries the final doc, the resolved data for chained relations, and root info.
type BuildDoc struct {
	Root model.Resource

	Doc      map[string]any
	Resolved map[string][]map[string]any

	Relations []model.Resource
}

// Plan is an aggregation executor that produces BuildDoc results.
type Plan = aggregation.Executer[BuildRequest, BuildDoc]
