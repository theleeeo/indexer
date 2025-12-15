package main

import "fmt"

type ResourceConfig struct {
	Resource  string           `yaml:"resource"`
	IndexName string           `yaml:"indexName"`
	Fields    []FieldConfig    `yaml:"fields"`
	Relations []RelationConfig `yaml:"relations"`
}

func (c ResourceConfig) Validate() error {
	if c.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if c.IndexName == "" {
		return fmt.Errorf("index_name required")
	}

	for i, f := range c.Fields {
		if err := f.Validate(); err != nil {
			if f.Name != "" {
				return fmt.Errorf("field %q: %w", f.Name, err)
			}
			return fmt.Errorf("field %d: %w", i, err)
		}
	}

	for i, r := range c.Relations {
		if err := r.Validate(); err != nil {
			if r.Resource != "" {
				return fmt.Errorf("relation %q: %w", r.Resource, err)
			}
			return fmt.Errorf("relation %d: %w", i, err)
		}
	}

	return nil
}

type FieldConfig struct {
	Name  string      `yaml:"name"`
	Query QueryConfig `yaml:"query"`
}

func (c FieldConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name required")
	}
	return nil
}

type QueryConfig struct {
	// Default true
	Search *bool `yaml:"search"`
}

type RelationKind string

const (
	RelationKindOne  RelationKind = "one"
	RelationKindMany RelationKind = "many"
)

type RelationConfig struct {
	Resource string        `yaml:"resource"`
	Kind     RelationKind  `yaml:"kind"`
	Fields   []FieldConfig `yaml:"fields"`
}

func (c RelationConfig) Validate() error {
	if c.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if c.Kind == "" {
		return fmt.Errorf("kind required")
	}

	if c.Kind != RelationKindOne && c.Kind != RelationKindMany {
		return fmt.Errorf("invalid kind: %s", c.Kind)
	}

	// TODO: Default to "Use all fields" if none specified?
	if len(c.Fields) == 0 {
		return fmt.Errorf("at least one field required")
	}

	for i, f := range c.Fields {
		if err := f.Validate(); err != nil {
			if f.Name != "" {
				return fmt.Errorf("field %q: %w", f.Name, err)
			}
			return fmt.Errorf("field %d: %w", i, err)
		}
	}

	return nil
}
