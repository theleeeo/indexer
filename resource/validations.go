package resource

import "fmt"

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

// Verifies that all relations are correctly defined and precalculates fields regarding bidirectional relations
func (c Configs) verifyFieldRelations() error {
	for _, rCfg := range c {
		for i, r := range rCfg.Relations {
			// Verify that the related resource exists
			relRCfg := c.Get(r.Resource)
			if relRCfg == nil {
				return fmt.Errorf("relation '%s' -> '%s' is specified but resource '%s' does not exist,", rCfg.Resource, r.Resource, r.Resource)
			}

			// Verify that the related resource has the fields defined in the relation
			for _, f := range r.Fields {
				found := false
				for _, relF := range relRCfg.Fields {
					if relF.Name == f.Name {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("relation '%s' -> '%s' specifies field '%s'.'%s' which does not exist", rCfg.Resource, r.Resource, r.Resource, f.Name)
				}
			}

			// If the related resource has a relation back to the original resource, mark both as bidirectional
			for j, relR := range relRCfg.Relations {
				if relR.Resource == rCfg.Resource {
					rCfg.Relations[i].Bidirectional = true
					relRCfg.Relations[j].Bidirectional = true
					break
				}
			}
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
