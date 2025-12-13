package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
)

type Client struct {
	es *elasticsearch.Client
}

type Config struct {
	Addresses []string
	Username  string
	Password  string
}

func New(cfg Config) (*Client, error) {
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: cfg.Addresses,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})
	if err != nil {
		return nil, err
	}
	return &Client{es: es}, nil
}

func (c *Client) UpsertJSON(ctx context.Context, indexAlias, docID string, doc any) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	res, err := c.es.Index(
		indexAlias,
		bytes.NewReader(body),
		c.es.Index.WithDocumentID(docID),
		c.es.Index.WithContext(ctx),
		c.es.Index.WithRefresh("false"),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es index error: %s %s", res.Status(), string(b))
	}
	log.Printf("UpsertJSON: indexed doc (id=%s, index=%s)", docID, indexAlias)
	return nil
}

func (c *Client) Delete(ctx context.Context, indexAlias, docID string) error {
	res, err := c.es.Delete(
		indexAlias,
		docID,
		c.es.Delete.WithContext(ctx),
		c.es.Delete.WithRefresh("false"),
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
		return fmt.Errorf("es delete error: %s %s", res.Status(), string(b))
	}
	log.Printf("Delete: deleted doc (id=%s, index=%s)", docID, indexAlias)
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
	enc := json.NewEncoder(&buf)

	for _, it := range items {
		meta := map[string]any{"index": map[string]any{"_index": it.Index, "_id": it.ID}}
		if err := enc.Encode(meta); err != nil {
			return err
		}
		if err := enc.Encode(it.Doc); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	res, err := c.es.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.es.Bulk.WithContext(ctx),
		c.es.Bulk.WithRefresh("false"),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("es bulk error: %s %s", res.Status(), string(b))
	}
	log.Printf("BulkUpsert: upserted %d docs", len(items))
	return nil
}
