package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/indexer/core"
	"github.com/theleeeo/indexer/gen/index/v1"
)

func TestProtoToNotification_Metadata(t *testing.T) {
	pn := &index.ChangeNotification{
		Kind:         index.ChangeKind_CHANGE_KIND_UPDATED,
		ResourceType: "order",
		ResourceId:   "42",
		Metadata: map[string]string{
			"tenant-id": "acme",
			"trace-id":  "trace-1",
		},
	}

	n := protoToNotification(pn)
	require.Equal(t, core.ChangeUpdated, n.Kind)
	require.Equal(t, "order", n.ResourceType)
	require.Equal(t, "42", n.ResourceID)
	require.Equal(t, pn.Metadata, n.Metadata)
}
