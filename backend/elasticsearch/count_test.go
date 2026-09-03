package elasticsearch

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountDocs_ReturnsCount(t *testing.T) {
	c := esClientReturning(t, http.StatusOK, `{"count":1234,"_shards":{"total":1}}`, func(req *http.Request) {
		require.True(t, strings.HasPrefix(req.URL.Path, "/my_index/_count"), "path %s", req.URL.Path)
	})

	count, err := c.CountDocs(context.Background(), "my_index")
	require.NoError(t, err)
	require.Equal(t, int64(1234), count)
}

func TestCountDocs_MissingIndexIsAnError(t *testing.T) {
	// Existence is a separate check (IndexExists); a count against a missing
	// index must fail loudly, not report zero.
	c := esClientReturning(t, http.StatusNotFound, `{"error":{"type":"index_not_found_exception"}}`, nil)

	_, err := c.CountDocs(context.Background(), "my_index")
	require.Error(t, err)
}

func TestCountDocs_ServerErrorPropagates(t *testing.T) {
	c := esClientReturning(t, http.StatusInternalServerError, `{"error":"boom"}`, nil)

	_, err := c.CountDocs(context.Background(), "my_index")
	require.Error(t, err)
}

func TestIndexExists_True(t *testing.T) {
	c := esClientReturning(t, http.StatusOK, ``, func(req *http.Request) {
		require.Equal(t, http.MethodHead, req.Method)
		require.Equal(t, "/my_index", req.URL.Path)
	})

	exists, err := c.IndexExists(context.Background(), "my_index")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestIndexExists_False(t *testing.T) {
	c := esClientReturning(t, http.StatusNotFound, ``, nil)

	exists, err := c.IndexExists(context.Background(), "my_index")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestIndexExists_ServerErrorPropagates(t *testing.T) {
	c := esClientReturning(t, http.StatusInternalServerError, ``, nil)

	_, err := c.IndexExists(context.Background(), "my_index")
	require.Error(t, err)
}
