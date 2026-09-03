// Command cleanup deletes a resource's de-configured versioned indices —
// the ones the given config no longer declares (ADR 0004 step 4, and the
// leftovers of a type dropped from config entirely).
//
// It is a dry run by default: pass -apply to actually delete. Every candidate
// is attempted — a failure is reported and does not stop the rest — and the
// exit code is non-zero when anything was refused or failed. An index still
// targeted by the read alias is always refused: a config that drops the
// version being served is stale or ahead of the deployment; cut over first.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/theleeeo/laika/app/config"
	"github.com/theleeeo/laika/core"
)

// cleanupAction classifies one versioned index against the config.
type cleanupAction int

const (
	// keepActive: the index belongs to a configured Schema Version.
	keepActive cleanupAction = iota
	// refuseAliasTarget: de-configured, but still the read alias target —
	// deleting it would break every reader. Never removed by this tool.
	refuseAliasTarget
	// removeIndex: de-configured and safe to delete.
	removeIndex
)

// classifyIndex decides what cleanup may do with one index.
func classifyIndex(index string, active map[string]bool, aliasTarget string) cleanupAction {
	if active[index] {
		return keepActive
	}
	if index == aliasTarget {
		return refuseAliasTarget
	}
	return removeIndex
}

func main() {
	configPath := flag.String("config", "resources.yml", "Path to resource config file")
	resourceName := flag.String("resource", "", "Resource name to clean up old indexes for (required; may be a type dropped from config)")
	esAddr := flag.String("es-addr", "http://localhost:9200", "Elasticsearch address")
	esUser := flag.String("es-user", "", "Elasticsearch username")
	esPass := flag.String("es-pass", "", "Elasticsearch password")
	apply := flag.Bool("apply", false, "Actually delete the candidate indices; without it this is a dry run")
	flag.Parse()

	if *resourceName == "" {
		flag.Usage()
		os.Exit(1)
	}

	resources, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load resource config: %v", err)
	}

	// A type dropped from config entirely has no active versions: every one
	// of its remaining indices is a candidate. The sweep leaves such indices
	// to this tool (it cannot know their version set).
	activeIndexes := map[string]bool{}
	if cfg := resources.Get(*resourceName); cfg != nil {
		for _, v := range cfg.SortedVersions() {
			activeIndexes[core.IndexName(cfg.Resource, v)] = true
		}
	} else {
		log.Printf("resource %q is not in the config; all of its versioned indices are candidates", *resourceName)
	}

	addr := strings.TrimRight(*esAddr, "/")
	aliasName := core.AliasName(*resourceName)

	aliasTarget, err := getAliasTarget(addr, aliasName, *esUser, *esPass)
	if err != nil {
		log.Fatalf("get alias target: %v", err)
	}

	indexes, err := listIndexes(addr, *resourceName+"_search_v", *esUser, *esPass)
	if err != nil {
		log.Fatalf("list indexes: %v", err)
	}

	var deleted, refused, failed int
	for _, idx := range indexes {
		switch classifyIndex(idx, activeIndexes, aliasTarget) {
		case keepActive:
			continue
		case refuseAliasTarget:
			log.Printf("REFUSING to delete %s: it is the current target of read alias %s — this config drops the version being served; cut over first", idx, aliasName)
			refused++
		case removeIndex:
			if !*apply {
				log.Printf("would delete %s (dry run; pass -apply to delete)", idx)
				deleted++
				continue
			}
			if err := deleteIndex(addr, idx, *esUser, *esPass); err != nil {
				log.Printf("delete %s failed: %v", idx, err)
				failed++
				continue
			}
			log.Printf("deleted %s", idx)
			deleted++
		}
	}

	verb := "deleted"
	if !*apply {
		verb = "would delete"
	}
	log.Printf("%s %d index(es); %d refused, %d failed", verb, deleted, refused, failed)
	if refused > 0 || failed > 0 {
		os.Exit(1)
	}
}

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

func listIndexes(addr, prefix, user, pass string) ([]string, error) {
	url := fmt.Sprintf("%s/_cat/indices/%s*?format=json", addr, prefix)
	statusCode, respBody, err := doRequest(http.MethodGet, url, nil, user, pass)
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
	}

	var indices []struct {
		Index string `json:"index"`
	}
	if err := json.Unmarshal(respBody, &indices); err != nil {
		return nil, err
	}

	names := make([]string, len(indices))
	for i, idx := range indices {
		names[i] = idx.Index
	}
	return names, nil
}

func deleteIndex(addr, indexName, user, pass string) error {
	url := fmt.Sprintf("%s/%s", addr, indexName)
	statusCode, respBody, err := doRequest(http.MethodDelete, url, nil, user, pass)
	if err != nil {
		return err
	}
	if statusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("unexpected response %d: %s", statusCode, string(respBody))
}

func doRequest(method, url string, body []byte, user, pass string) (int, []byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = strings.NewReader(string(body))
	}
	req, err := http.NewRequest(method, url, bodyReader)
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
