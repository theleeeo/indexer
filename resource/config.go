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
	Type  string      `yaml:"type"` // ES field type; defaults to "keyword"
	Query QueryConfig `yaml:"query"`
}

func (f FieldConfig) ESType() string {
	if f.Type == "" {
		return "keyword"
	}
	return f.Type
}

type QueryConfig struct {
	// Default true
	Search *bool `yaml:"search"`
}

type KeyConfig struct {
	// Source is the name of the resource to extract the lookup key from.
	// Use the root resource's own name to extract from root data,
	// or a sibling relation name to extract from its resolved data.
	Source string `yaml:"source"`

	// Fields are the field names to extract from the source resource's data.
	// All extracted values are passed together to the provider.
	Fields []string `yaml:"fields"`
}

type RelationConfig struct {
	Resource    string        `yaml:"resource"`
	Key         KeyConfig     `yaml:"key"`
	Cardinality string        `yaml:"cardinality"` // "one" or "many"; defaults to "many"
	Fields      []FieldConfig `yaml:"fields"`
}

func (r RelationConfig) IsMany() bool {
	return r.Cardinality != "one"
}
