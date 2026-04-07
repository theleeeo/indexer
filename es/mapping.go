package es

import "github.com/theleeeo/indexer/resource"

// GenerateMapping builds an Elasticsearch index mapping for a single resource config.
func GenerateMapping(cfg *resource.Config) map[string]any {
	fieldsProps := make(map[string]any, len(cfg.Fields))
	for _, f := range cfg.Fields {
		fieldsProps[f.Name] = map[string]any{
			"type": f.ESType(),
		}
	}

	properties := map[string]any{
		"fields": map[string]any{
			"type":       "object",
			"properties": fieldsProps,
		},
	}

	for _, rel := range cfg.Relations {
		relProps := make(map[string]any, len(rel.Fields)+1)
		relProps["id"] = map[string]any{"type": "keyword"}
		for _, f := range rel.Fields {
			relProps[f.Name] = map[string]any{
				"type": f.ESType(),
			}
		}

		relType := "nested"
		if !rel.IsMany() {
			relType = "object"
		}

		properties[rel.Resource] = map[string]any{
			"type":       relType,
			"properties": relProps,
		}
	}

	return map[string]any{
		"mappings": map[string]any{
			"properties": properties,
		},
	}
}

// GenerateMappings builds ES index mappings for all resource configs.
// Returns a map of index name -> mapping.
func GenerateMappings(configs resource.Configs) map[string]map[string]any {
	result := make(map[string]map[string]any, len(configs))
	for _, cfg := range configs {
		indexName := cfg.Resource + "_search"
		result[indexName] = GenerateMapping(cfg)
	}
	return result
}
