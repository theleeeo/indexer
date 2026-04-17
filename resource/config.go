package resource

import (
	"fmt"
	"sort"
)

type Configs []*Config

func (c Configs) Get(resource string) *Config {
	for _, rc := range c {
		if rc.Resource == resource {
			return rc
		}
	}
	return nil
}

// VersionConfig holds the schema definition for a single version of a resource.
type VersionConfig struct {
	Fields    []FieldConfig    `yaml:"fields"`
	Relations []RelationConfig `yaml:"relations"`
}

type Config struct {
	Resource string `yaml:"resource"`

	// Per-version schema definitions. Keys are version numbers (positive ints).
	// When omitted, the top-level Fields/Relations are treated as version 1.
	VersionDefs map[int]*VersionConfig `yaml:"versions,omitempty"`

	// ReadVersion is the version whose index the read alias points to.
	// Must be one of the VersionDefs keys. Defaults to the lowest version.
	ReadVersion int `yaml:"readVersion"`

	// Top-level fields/relations — for backward compatibility with configs
	// that don't use versioned definitions. After ApplyDefaults, these are
	// always populated from the ReadVersion's VersionConfig.
	Fields    []FieldConfig    `yaml:"fields,omitempty"`
	Relations []RelationConfig `yaml:"relations,omitempty"`
}

// SortedVersions returns the version numbers in ascending order.
func (c *Config) SortedVersions() []int {
	versions := make([]int, 0, len(c.VersionDefs))
	for v := range c.VersionDefs {
		versions = append(versions, v)
	}
	sort.Ints(versions)
	return versions
}

// HasRelationTo reports whether any version of this resource has a relation
// to the given resource type.
func (c *Config) HasRelationTo(resourceType string) bool {
	for _, vc := range c.VersionDefs {
		for _, rel := range vc.Relations {
			if rel.Resource == resourceType {
				return true
			}
		}
	}
	return false
}

// ApplyDefaults fills in zero-value fields with sensible defaults.
// Call this after unmarshalling config from YAML.
func (c *Config) ApplyDefaults() {
	// If no versioned definitions, promote top-level fields/relations to version 1.
	if len(c.VersionDefs) == 0 {
		c.VersionDefs = map[int]*VersionConfig{
			1: {Fields: c.Fields, Relations: c.Relations},
		}
	}

	if c.ReadVersion == 0 {
		c.ReadVersion = c.SortedVersions()[0]
	}

	// Populate top-level Fields/Relations from the read version's config
	// so existing code (search, validation, etc.) works unchanged.
	rv := c.VersionDefs[c.ReadVersion]
	if rv != nil {
		c.Fields = rv.Fields
		c.Relations = rv.Relations
	}
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
