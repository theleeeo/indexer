package resource

import "fmt"

type Configs []*Config

func (c Configs) Get(resource string) *Config {
	for _, rc := range c {
		if rc.Resource == resource {
			return rc
		}
	}
	return nil
}

type Config struct {
	Resource  string           `yaml:"resource"`
	Fields    []FieldConfig    `yaml:"fields"`
	Relations []RelationConfig `yaml:"relations"`
}

func (c Config) GetSearchableFields() []string {
	var fields []string
	for _, f := range c.Fields {
		if f.Query.Search == nil || *f.Query.Search {
			fields = append(fields, "fields."+f.Name)
		}
	}

	for _, r := range c.Relations {
		for _, f := range r.Fields {
			if f.Query.Search == nil || *f.Query.Search {
				fields = append(fields, fmt.Sprintf("%s.%s", r.Resource, f.Name))
			}
		}
	}

	return fields
}

func (c Config) GetRelation(resource string) *RelationConfig {
	for _, r := range c.Relations {
		if r.Resource == resource {
			return &r
		}
	}
	return nil
}

type FieldConfig struct {
	Name  string      `yaml:"name"`
	Query QueryConfig `yaml:"query"`
}

type QueryConfig struct {
	// Default true
	Search *bool `yaml:"search"`
}

type RelationConfig struct {
	Resource string        `yaml:"resource"`
	Fields   []FieldConfig `yaml:"fields"`
	// If set, this relation is dependent on this relation on this resource.
	// IE. if A has a relation to C with dependance B, when a B is added to A, the C on that B should be added to A as well.
	// This is used for relations that are not directly represented in the data, but can be inferred from other relations.
	Dependance string `yaml:"dependance"`

	// Calculated:

	// If true, the related resource has a relation back to the original resource, so updates should be propagated in both directions
	Bidirectional bool

	// UpdateResources holds the list of related resources that should be updated when this relation is updated.
	// This is calculated based on the Dependance field and is used to propagate updates to dependent resources.
	UpdateResources []string
}
