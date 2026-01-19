package resource

import "fmt"

type Config struct {
	Resource  string           `yaml:"resource"`
	Fields    []FieldConfig    `yaml:"fields"`
	Relations []RelationConfig `yaml:"relations"`
}

func (c Config) Validate() error {
	if c.Resource == "" {
		return fmt.Errorf("resource required")
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

type RelationConfig struct {
	Resource string `yaml:"resource"`
	// TODO: Implement
	Bidirectional bool          `yaml:"bidirectional"`
	Fields        []FieldConfig `yaml:"fields"`
}

func (c RelationConfig) Validate() error {
	if c.Resource == "" {
		return fmt.Errorf("resource required")
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
