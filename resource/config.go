package resource

import "fmt"

type Configs []*Config

func (c Configs) Get(resourceName string) *Config {
	for _, rc := range c {
		if rc.Resource == resourceName {
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

	// Calculated
	Bidirectional bool
}
