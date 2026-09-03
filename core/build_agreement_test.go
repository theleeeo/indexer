package core

// Cross-version agreement in multi-Schema-Version builds (ADR 0004): all of a
// resource's plans must agree it exists before anything is written, one
// version's nil result must never delete the other versions' documents, and
// parent discovery (ADR 0006) must union over every plan — not just the last.

import (
	"context"
	"testing"

	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// nilDoc is a plan result whose source returned no data: the root is known
// but the projected document is nil.
func nilDoc(id string) projection.BuildDoc {
	return projection.BuildDoc{Root: model.Resource{Type: "product", Id: id}}
}

// count reports how many recorded calls start with prefix.
func (s *rebuildRecordingStore) count(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if len(c) >= len(prefix) && c[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

func TestBuild_PlansDisagreeOnExistence_LeavesStaleInsteadOfDeleting(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	// Build reports per-id failures via logs, not its error; the contract is
	// in the effects below.
	if err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}

	if ds := es.deletesSnapshot(); len(ds) != 0 {
		t.Fatalf("plans disagreeing on existence must not delete any version's document, deleted %v", ds)
	}
	if len(es.upserts) != 0 {
		t.Fatalf("existence must be decided before any write, wrote %v", es.upserts)
	}
	if st.has("ClearStale:product/1") {
		t.Fatal("the resource must stay stale so a retry can converge on the source's real state")
	}
}

func TestBuild_AllPlansNil_DeletesEveryVersion(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	if err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}

	ds := es.deletesSnapshot()
	want := map[string]bool{"product_search_v1/1": false, "product_search_v2/1": false}
	for _, d := range ds {
		want[d] = true
	}
	for index, deleted := range want {
		if !deleted {
			t.Fatalf("unanimous nil means the resource is gone: %s must be deleted (got %v)", index, ds)
		}
	}
}

func TestBuild_ParentsCollectedFromEveryPlan(t *testing.T) {
	withParents := func(d projection.BuildDoc, parents ...model.Resource) projection.BuildDoc {
		d.Parents = parents
		return d
	}
	parentA := model.Resource{Type: "parent", Id: "a"}
	parentB := model.Resource{Type: "parent", Id: "b"}

	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		// parentA appears in both plans; parentB only in the first — the union
		// must keep both, once each.
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{withParents(productDoc("1"), parentB, parentA)}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{withParents(productDoc("1"), parentA)}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	if err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}}); err != nil {
		t.Fatal(err)
	}

	if !st.has("MarkStale:parent/b") {
		t.Fatalf("a parent discovered by a non-final plan must still be scheduled (ADR 0006): %v", st.calls)
	}
	if got := st.count("MarkStale:parent/a"); got != 1 {
		t.Fatalf("a parent discovered by several plans must be scheduled once, got %d marks", got)
	}
}

func TestRebuildByIDs_PlansDisagreeOnExistence_LeavesStale(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{nilDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", ResourceIDs: []string{"1"}},
	})
	if err == nil {
		t.Fatal("a rebuild that could not settle a resource must report failure")
	}

	if ds := es.deletesSnapshot(); len(ds) != 0 {
		t.Fatalf("plans disagreeing on existence must not delete any version's document, deleted %v", ds)
	}
	if items := es.allBulkItems(); len(items) != 0 {
		t.Fatalf("no version's document may be written for an unsettled resource, wrote %v", items)
	}
	if st.has("ClearStale:product/1") {
		t.Fatal("the resource must stay stale for the sweep")
	}
	if !st.has("MarkStale:product/1") {
		t.Fatal("the failed resource must be durably re-marked stale")
	}
}
