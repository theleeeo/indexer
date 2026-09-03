package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/theleeeo/laika/app/config"
	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

func main() {
	configPath := flag.String("config", "resources.yml", "Path to resource config file")
	index := flag.String("index", "", "Resource name to generate (e.g. \"a\"); omit for all")
	apply := flag.String("apply", "", "Elasticsearch address to apply the mapping to (e.g. http://localhost:9200)")
	esUser := flag.String("es-user", "", "Elasticsearch username")
	esPass := flag.String("es-pass", "", "Elasticsearch password")
	force := flag.Bool("force", false, "Move a read alias backwards or off a hand-built index (a stale -config file is the usual cause of needing this)")
	flag.Parse()

	resources, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}
	if err := resources.Validate(); err != nil {
		log.Fatalf("invalid resource config: %v", err)
	}

	// Build the set of mappings to work with.
	mappings := map[string]map[string]any{}
	if *index != "" {
		cfg := resources.Get(*index)
		if cfg == nil {
			log.Fatalf("unknown resource %q", *index)
		}
		for _, vc := range cfg.Versions {
			indexName := core.IndexName(cfg.Resource, vc.Version)
			mappings[indexName] = elasticsearch.GenerateMapping(&vc)
		}
	} else {
		mappings = elasticsearch.GenerateMappings(resources)
	}

	if *apply != "" {
		addr := strings.TrimRight(*apply, "/")
		for indexName, mapping := range mappings {
			if err := applyMapping(addr, indexName, mapping, *esUser, *esPass); err != nil {
				log.Fatalf("apply mapping for %s: %v", indexName, err)
			}
			log.Printf("applied mapping to %s", indexName)
		}

		// Converge read aliases onto the readVersion index. The config owns the
		// alias target (ADR 0009), but this tool runs against whatever -config
		// file it is handed — so a move that would undo a cutover (backwards, or
		// off a hand-built alias) is refused unless -force says the file is
		// really the current truth.
		targetResources := resources
		if *index != "" {
			targetResources = resource.Configs{resources.Get(*index)}
		}
		for _, cfg := range targetResources {
			aliasName := core.AliasName(cfg.Resource)
			targetIndex := core.IndexName(cfg.Resource, cfg.ReadVersion)

			current, err := getAliasTarget(addr, aliasName, *esUser, *esPass)
			if err != nil {
				log.Fatalf("read alias %s: %v", aliasName, err)
			}

			move := core.PlanAliasMove(cfg.Resource, current, cfg.ReadVersion)
			applyMove, err := decideAliasApply(move, *force)
			if err != nil {
				log.Fatalf("alias %s: currently %s, config wants %s: %v", aliasName, current, targetIndex, err)
			}
			if !applyMove {
				log.Printf("alias %s already points to %s", aliasName, targetIndex)
				continue
			}

			if err := applyAlias(addr, aliasName, targetIndex, *esUser, *esPass); err != nil {
				log.Fatalf("apply alias %s -> %s: %v", aliasName, targetIndex, err)
			}
			log.Printf("alias %s -> %s", aliasName, targetIndex)
		}
		return
	}

	// Default: print to stdout.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(mappings); err != nil {
		log.Fatalf("encode mappings: %v", err)
	}
}

// decideAliasApply is the write policy on top of core.PlanAliasMove: apply
// creations and forward cutovers, skip an in-sync alias, and refuse a
// backwards move or a hand-built target unless forced.
func decideAliasApply(move core.AliasMove, force bool) (bool, error) {
	switch move {
	case core.AliasInSync:
		return false, nil
	case core.AliasCreate, core.AliasForward:
		return true, nil
	case core.AliasBackward:
		if force {
			return true, nil
		}
		return false, fmt.Errorf("refusing to move the read alias backwards; is this -config file stale? rerun with -force to roll back")
	case core.AliasForeign:
		if force {
			return true, nil
		}
		return false, fmt.Errorf("refusing to move the read alias off an index this config does not own; rerun with -force to take it over")
	default:
		return false, fmt.Errorf("unknown alias move %d", move)
	}
}

// getAliasTarget returns the concrete index the alias currently points to,
// or "" if the alias does not exist.
func getAliasTarget(addr, aliasName, user, pass string) (string, error) {
	url := fmt.Sprintf("%s/_alias/%s", addr, aliasName)
	statusCode, respBody, err := doRequest(http.MethodGet, url, nil, user, pass)
	if err != nil {
		return "", err
	}
	if statusCode == http.StatusNotFound {
		return "", nil
	}
	if statusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
	}

	var decoded map[string]any
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return "", err
	}
	for indexName := range decoded {
		return indexName, nil
	}
	return "", nil
}

// applyMapping PUTs the mapping body to ES. It first tries to create the index;
// if it already exists (409) it falls back to the _mapping endpoint to update.
func applyMapping(addr, indexName string, mapping map[string]any, user, pass string) error {
	body, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	// Try to create the index with the full mapping.
	url := fmt.Sprintf("%s/%s", addr, indexName)
	statusCode, respBody, err := doRequest(http.MethodPut, url, body, user, pass)
	if err != nil {
		return err
	}
	if statusCode == http.StatusOK || statusCode == http.StatusCreated {
		return nil
	}

	// 400 with resource_already_exists_exception — index exists, update the mapping instead.
	if statusCode == http.StatusBadRequest && strings.Contains(string(respBody), "resource_already_exists_exception") {
		mappingBody, err := json.Marshal(mapping["mappings"])
		if err != nil {
			return err
		}
		updateURL := fmt.Sprintf("%s/%s/_mapping", addr, indexName)
		statusCode, respBody, err = doRequest(http.MethodPut, updateURL, mappingBody, user, pass)
		if err != nil {
			return err
		}
		if statusCode == http.StatusOK {
			return nil
		}
	}

	return fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
}

func doRequest(method, url string, body []byte, user, pass string) (int, []byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if user != "" {
		req.SetBasicAuth(user, pass)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

// applyAlias creates or updates an ES alias to point to the given index using
// the _aliases API. It first removes any existing targets of the alias, then
// adds the new target atomically.
func applyAlias(addr, aliasName, indexName, user, pass string) error {
	body := map[string]any{
		"actions": []any{
			map[string]any{
				"remove": map[string]any{
					"index": "*",
					"alias": aliasName,
				},
			},
			map[string]any{
				"add": map[string]any{
					"index": indexName,
					"alias": aliasName,
				},
			},
		},
	}

	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/_aliases", addr)
	statusCode, respBody, err := doRequest(http.MethodPost, url, b, user, pass)
	if err != nil {
		return err
	}
	if statusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
}
