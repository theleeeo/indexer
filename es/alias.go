package es

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
)

// IndexName returns the concrete versioned index name for a resource and version.
// Example: IndexName("a", 2) → "a_search_v2"
func IndexName(resource string, version int) string {
	return fmt.Sprintf("%s_search_v%d", resource, version)
}

// AliasName returns the read alias name for a resource.
// Example: AliasName("a") → "a_search"
func AliasName(resource string) string {
	return resource + "_search"
}

// IndexNames returns the concrete versioned index names for all given versions.
func IndexNames(resource string, versions []int) []string {
	names := make([]string, len(versions))
	for i, v := range versions {
		names[i] = IndexName(resource, v)
	}
	return names
}

// CreateAlias creates an alias pointing to the given index.
// If the alias already exists pointing elsewhere, it is moved atomically.
func (c *Client) CreateAlias(ctx context.Context, aliasName, indexName string) error {
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
		return fmt.Errorf("marshal alias body: %w", err)
	}

	res, err := c.es.Indices.UpdateAliases(
		bytes.NewReader(b),
		c.es.Indices.UpdateAliases.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("update aliases: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return fmt.Errorf("update aliases error: %s %s", res.Status(), string(raw))
	}

	return nil
}

// SwitchAlias atomically moves an alias from one index to another.
func (c *Client) SwitchAlias(ctx context.Context, aliasName, fromIndex, toIndex string) error {
	body := map[string]any{
		"actions": []any{
			map[string]any{
				"remove": map[string]any{
					"index": fromIndex,
					"alias": aliasName,
				},
			},
			map[string]any{
				"add": map[string]any{
					"index": toIndex,
					"alias": aliasName,
				},
			},
		},
	}

	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal alias body: %w", err)
	}

	res, err := c.es.Indices.UpdateAliases(
		bytes.NewReader(b),
		c.es.Indices.UpdateAliases.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("update aliases: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return fmt.Errorf("switch alias error: %s %s", res.Status(), string(raw))
	}

	return nil
}

// GetAlias returns the concrete index name that the alias currently points to.
// Returns empty string and no error if the alias does not exist.
func (c *Client) GetAlias(ctx context.Context, aliasName string) (string, error) {
	res, err := c.es.Indices.GetAlias(
		c.es.Indices.GetAlias.WithName(aliasName),
		c.es.Indices.GetAlias.WithContext(ctx),
	)
	if err != nil {
		return "", fmt.Errorf("get alias: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return "", nil
	}

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return "", fmt.Errorf("get alias error: %s %s", res.Status(), string(raw))
	}

	// Response shape: { "index_name": { "aliases": { "alias_name": {} } } }
	var decoded map[string]any
	if err := json.UnmarshalRead(res.Body, &decoded); err != nil {
		return "", fmt.Errorf("decode alias response: %w", err)
	}

	for indexName := range decoded {
		return indexName, nil
	}

	return "", nil
}

// DeleteIndex deletes an Elasticsearch index.
func (c *Client) DeleteIndex(ctx context.Context, indexName string) error {
	res, err := c.es.Indices.Delete(
		[]string{indexName},
		c.es.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("delete index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return fmt.Errorf("delete index error: %s %s", res.Status(), string(raw))
	}

	return nil
}
