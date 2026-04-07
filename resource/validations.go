package resource

import (
	"fmt"
	"slices"
)

func (c Configs) Validate() error {
	if len(c) == 0 {
		return fmt.Errorf("at least one resource config required")
	}

	// Verify that every individual config is valid
	for i, rc := range c {
		if err := rc.Validate(); err != nil {
			if rc.Resource != "" {
				return fmt.Errorf("resource %q: %w", rc.Resource, err)
			}
			return fmt.Errorf("resource %d: %w", i, err)
		}
	}

	if err := c.verifyFieldRelations(); err != nil {
		return err
	}

	return nil
}

// verifyFieldRelations verifies that all relations reference existing resources,
// that their field lists match the target resource's field definitions,
// that key sources reference valid resources, and that there are no dependency cycles.
func (c Configs) verifyFieldRelations() error {
	for _, rCfg := range c {
		for _, currentRel := range rCfg.Relations {
			// Verify that the related resource exists
			relRCfg := c.Get(currentRel.Resource)
			if relRCfg == nil {
				return fmt.Errorf("relation '%s'->'%s' is specified but resource '%s' does not exist", rCfg.Resource, currentRel.Resource, currentRel.Resource)
			}

			// Verify that the related resource has the fields defined in the relation
			for _, f := range currentRel.Fields {
				if !slices.ContainsFunc(relRCfg.Fields, func(relF FieldConfig) bool {
					return relF.Name == f.Name
				}) {
					return fmt.Errorf("relation '%s'->'%s' specifies field '%s' which does not exist on '%s'", rCfg.Resource, currentRel.Resource, f.Name, currentRel.Resource)
				}
			}

			// Verify key source: must be the owning resource or a sibling relation
			if currentRel.Key.Source != rCfg.Resource {
				found := false
				for _, siblingRel := range rCfg.Relations {
					if siblingRel.Resource == currentRel.Key.Source {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("relation '%s'->'%s' key source '%s' is not the root resource and not a sibling relation", rCfg.Resource, currentRel.Resource, currentRel.Key.Source)
				}
			}
		}

		// Verify no cycles in relation key dependencies
		if err := verifyNoCycles(rCfg); err != nil {
			return err
		}
	}

	return nil
}

// verifyNoCycles checks that the relation key dependencies within a single
// resource config form a DAG (no cycles).
func verifyNoCycles(rCfg *Config) error {
	// Build adjacency: relation resource name -> list of deps (key sources that are sibling relations)
	deps := make(map[string]string) // relation -> dependency (its key source, if it's a sibling)
	for _, rel := range rCfg.Relations {
		if rel.Key.Source != rCfg.Resource {
			deps[rel.Resource] = rel.Key.Source
		}
	}

	// Walk each chain to detect cycles
	visited := make(map[string]bool)
	inStack := make(map[string]bool)

	var visit func(node string) error
	visit = func(node string) error {
		if inStack[node] {
			return fmt.Errorf("resource %q has a cycle in relation key dependencies involving %q", rCfg.Resource, node)
		}
		if visited[node] {
			return nil
		}
		visited[node] = true
		inStack[node] = true

		if dep, ok := deps[node]; ok {
			if err := visit(dep); err != nil {
				return err
			}
		}

		inStack[node] = false
		return nil
	}

	for _, rel := range rCfg.Relations {
		if err := visit(rel.Resource); err != nil {
			return err
		}
	}

	return nil
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

func (c FieldConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name required")
	}
	return nil
}

func (c RelationConfig) Validate() error {
	if c.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if err := c.Key.Validate(); err != nil {
		return fmt.Errorf("key: %w", err)
	}

	if c.Cardinality != "" && c.Cardinality != "one" && c.Cardinality != "many" {
		return fmt.Errorf("cardinality must be \"one\" or \"many\"")
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

func (k KeyConfig) Validate() error {
	if k.Source == "" {
		return fmt.Errorf("source required")
	}
	if len(k.Fields) == 0 {
		return fmt.Errorf("at least one field required")
	}
	for i, f := range k.Fields {
		if f == "" {
			return fmt.Errorf("field %d: name required", i)
		}
	}
	return nil
}
