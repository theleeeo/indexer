package es

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
)

var ErrNotFound = fmt.Errorf("document not found")

type Client struct {
	es *elasticsearch.Client

	// Temporary solution to control refresh behavior during tests
	withRefresh bool
}

func New(client *elasticsearch.Client, withRefresh bool) *Client {
	return &Client{es: client, withRefresh: withRefresh}
}

func (c *Client) Upsert(ctx context.Context, indexAlias, docID string, doc any) error {
	now := time.Now()
	defer func() {
		slog.Info("upserted doc", "docID", docID, "index", indexAlias, "duration", time.Since(now))
	}()

	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Index(
		indexAlias,
		bytes.NewReader(body),
		c.es.Index.WithDocumentID(docID),
		c.es.Index.WithContext(ctx),
		c.es.Index.WithRefresh(refresh),
		// c.es.Index.WithOpType("create"), // Only create, fail if exists
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, indexAlias, docID string) error {
	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Delete(
		indexAlias,
		docID,
		c.es.Delete.WithContext(ctx),
		c.es.Delete.WithRefresh(refresh),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil
	}
	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}
	slog.Info("deleted doc", "docID", docID, "index", indexAlias)
	return nil
}

type BulkItem struct {
	Index string
	ID    string
	Doc   any
}

func (c *Client) BulkUpsert(ctx context.Context, items []BulkItem) error {
	if len(items) == 0 {
		return nil
	}

	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf)

	for _, it := range items {
		meta := map[string]any{"index": map[string]any{"_index": it.Index, "_id": it.ID}}
		if err := json.MarshalEncode(enc, meta); err != nil {
			return fmt.Errorf("marshal index meta: %w", err)
		}

		if err := json.MarshalEncode(enc, it.Doc); err != nil {
			return fmt.Errorf("marshal doc: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.es.Bulk.WithContext(ctx),
		c.es.Bulk.WithRefresh(refresh),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es bulk error: %s %s", res.Status(), string(b))
	}
	slog.Info("bulk upserted docs", "count", len(items))
	return nil
}

func (c *Client) UpdateField(ctx context.Context, indexAlias, docID, field string, value any) error {
	updateBody := map[string]any{
		"doc": map[string]any{
			field: value,
		},
	}
	body, err := json.Marshal(updateBody)
	if err != nil {
		return err
	}

	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Update(
		indexAlias,
		docID,
		bytes.NewReader(body),
		c.es.Update.WithContext(ctx),
		c.es.Update.WithRefresh(refresh),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}
	slog.Info("updated field", "field", field, "docID", docID, "index", indexAlias)
	return nil
}

func (c *Client) UpsertFieldResourceById(ctx context.Context, indexAlias, docID, field string, elementId string, newElement any) error {
	if elementId == "" {
		return fmt.Errorf("elementId required")
	}

	if newElement == nil {
		newElement = map[string]any{
			"id": elementId,
		}
	}

	script := `
		params.new_element['id'] = params.element_id;  // ensure id is always set
		if (ctx._source[params.field] == null) {
			ctx._source[params.field] = [params.new_element];
		} else {
			def found = false;
			for (int i = 0; i < ctx._source[params.field].length; i++) {
				if (ctx._source[params.field][i].id == params.element_id) {
					ctx._source[params.field][i] = params.new_element;
					found = true;
					break;
				}
			}
			if (!found) {
				ctx._source[params.field].add(params.new_element);
			}
		}
	`

	updateBody := map[string]any{
		"script": map[string]any{
			"source": script,
			"lang":   "painless",
			"params": map[string]any{
				"field":       field,
				"element_id":  elementId,
				"new_element": newElement,
			},
		},
	}

	body, err := json.Marshal(updateBody)
	if err != nil {
		return err
	}

	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Update(
		indexAlias,
		docID,
		bytes.NewReader(body),
		c.es.Update.WithContext(ctx),
		c.es.Update.WithRefresh(refresh),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return ErrNotFound
		}
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}
	slog.Info("upserted field resource by id", "elementID", elementId, "field", field, "docID", docID, "index", indexAlias)
	return nil
}

func (c *Client) AddFieldResource(ctx context.Context, indexAlias, docID, field string, newElement any) error {
	if newElement == nil {
		return fmt.Errorf("newElement required")
	}

	script := `
		if (ctx._source[params.field] == null) {
			ctx._source[params.field] = [params.new_element];
		} else {
			ctx._source[params.field].add(params.new_element);
		}
	`

	updateBody := map[string]any{
		"script": map[string]any{
			"source": script,
			"lang":   "painless",
			"params": map[string]any{
				"field":       field,
				"new_element": newElement,
			},
		},
	}

	body, err := json.Marshal(updateBody)
	if err != nil {
		return err
	}

	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Update(
		indexAlias,
		docID,
		bytes.NewReader(body),
		c.es.Update.WithContext(ctx),
		c.es.Update.WithRefresh(refresh),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}
	slog.Info("added field resource", "field", field, "docID", docID, "index", indexAlias)
	return nil
}

func (c *Client) RemoveFieldResourceById(ctx context.Context, indexAlias, docID, field string, elementID any) error {
	script := `
		def f = ctx._source[params.field];
		if (f != null) {
			if (f instanceof List) {
				f.removeIf(e -> e != null && e.id == params.element_id);
			} else if (f instanceof Map && f.id == params.element_id) {
				ctx._source.remove(params.field);
			}
		}
	`

	updateBody := map[string]any{
		"script": map[string]any{
			"source": script,
			"lang":   "painless",
			"params": map[string]any{
				"field":      field,
				"element_id": elementID,
			},
		},
	}

	body, err := json.Marshal(updateBody)
	if err != nil {
		return err
	}

	refresh := "false"
	if c.withRefresh {
		refresh = "true"
	}

	res, err := c.es.Update(
		indexAlias,
		docID,
		bytes.NewReader(body),
		c.es.Update.WithContext(ctx),
		c.es.Update.WithRefresh(refresh),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}
	slog.Info("removed field resource by id", "elementID", elementID, "field", field, "docID", docID, "index", indexAlias)
	return nil
}

func (c *Client) Get(ctx context.Context, indexAlias, docID string) (map[string]any, error) {
	res, err := c.es.Get(
		indexAlias,
		docID,
		c.es.Get.WithContext(ctx),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, nil
	}

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("es error: %s %s", res.Status(), string(b))
	}

	var getRes struct {
		Source map[string]any `json:"_source"`
	}
	if err := json.UnmarshalRead(res.Body, &getRes); err != nil {
		return nil, err
	}

	return getRes.Source, nil
}
