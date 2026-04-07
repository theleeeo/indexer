package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/resource"

	"github.com/goccy/go-yaml"
)

func main() {
	configPath := "resources.yml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	resources, err := loadResourceConfig(configPath)
	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}

	if err := resources.Validate(); err != nil {
		log.Fatalf("invalid resource config: %v", err)
	}

	mappings := es.GenerateMappings(resources)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(mappings); err != nil {
		log.Fatalf("encode mappings: %v", err)
	}
}

func loadResourceConfig(path string) (resource.Configs, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}
	var cfg map[string]*resource.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	resources := make([]*resource.Config, 0, len(cfg))
	for name, rc := range cfg {
		rc.Resource = name
		resources = append(resources, rc)
	}

	return resources, nil
}
