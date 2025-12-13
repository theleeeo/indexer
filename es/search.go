package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type SearchResult struct {
	Total int64
	Hits  []Hit
}

type Hit struct {
	ID     string
	Score  float64
	Source map[string]any
}

func (c *Client) Search(ctx context.Context, indexAlias string, body map[string]any) (*SearchResult, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	res, err := c.es.Search(
		c.es.Search.WithContext(ctx),
		c.es.Search.WithIndex(indexAlias),
		c.es.Search.WithBody(bytes.NewReader(b)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		raw, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("es search error: %s %s", res.Status(), string(raw))
	}

	var decoded map[string]any
	if err := json.NewDecoder(res.Body).Decode(&decoded); err != nil {
		return nil, err
	}

	hitsObj, _ := decoded["hits"].(map[string]any)

	// total: { "value": N, "relation": "eq" } (ES7+)
	var total int64
	if t, ok := hitsObj["total"].(map[string]any); ok {
		if v, ok := t["value"].(float64); ok {
			total = int64(v)
		}
	}

	hitsArr, _ := hitsObj["hits"].([]any)
	out := &SearchResult{Total: total}

	for _, h := range hitsArr {
		m, _ := h.(map[string]any)
		id, _ := m["_id"].(string)
		score, _ := m["_score"].(float64)
		src, _ := m["_source"].(map[string]any)

		out.Hits = append(out.Hits, Hit{
			ID:     id,
			Score:  score,
			Source: src,
		})
	}

	return out, nil
}
