package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/theleeeo/laika/aggregation"
	"github.com/theleeeo/laika/core/resource"
	"github.com/theleeeo/laika/model"
	"github.com/theleeeo/laika/projection"
)

// twoVersionResources returns a "product" type with Schema Versions 1 and 2 —
// the ADR 0004 migration shape (v2 adds a field alongside the serving v1).
func twoVersionResources() resource.Configs {
	cfgs := resource.Configs{
		{
			Resource: "product",
			Versions: []resource.VersionConfig{
				{Version: 1, Fields: []resource.FieldConfig{{Name: "title", Type: "text"}}},
				{Version: 2, Fields: []resource.FieldConfig{
					{Name: "title", Type: "text"},
					{Name: "price", Type: "long"},
				}},
			},
		},
	}
	for _, c := range cfgs {
		c.ApplyDefaults()
	}
	return cfgs
}

func productDoc(id string, rels ...model.VersionedResource) projection.BuildDoc {
	return projection.BuildDoc{
		Root:      model.Resource{Type: "product", Id: id},
		Doc:       map[string]any{"fields": map[string]any{"title": "t-" + id}},
		Relations: rels,
	}
}

// pagingExecuter emits the given pages in order, mimicking an all-of-type
// walk's paginated ListResources stream.
type pagingExecuter struct {
	pages []aggregation.ExecutionResult[projection.BuildDoc]
}

func (e *pagingExecuter) Execute(context.Context, projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], len(e.pages))
	for _, p := range e.pages {
		ch <- p
	}
	close(ch)
	return ch
}

// captureBackend records every write and can reject specific document IDs in
// bulk responses or answer single upserts with a version conflict.
type captureBackend struct {
	mu             sync.Mutex
	bulkCalls      [][]BulkItem
	deletes        []string        // "index/id"
	upserts        []string        // "index/id"
	rejectIDs      map[string]bool // BulkUpsert reports these IDs as rejected
	upsertConflict map[string]bool // Upsert returns ErrVersionConflict for "index/id"
}

func (b *captureBackend) Upsert(_ context.Context, index, docID string, _ any, _ int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := index + "/" + docID
	b.upserts = append(b.upserts, key)
	if b.upsertConflict[key] {
		return fmt.Errorf("superseded: %w", ErrVersionConflict)
	}
	return nil
}

func (b *captureBackend) BulkUpsert(_ context.Context, items []BulkItem) ([]BulkFailure, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bulkCalls = append(b.bulkCalls, append([]BulkItem(nil), items...))
	var failures []BulkFailure
	for _, it := range items {
		if b.rejectIDs[it.ID] {
			failures = append(failures, BulkFailure{Index: it.Index, ID: it.ID, Status: 400, Reason: "test rejection"})
		}
	}
	return failures, nil
}

func (b *captureBackend) Delete(_ context.Context, index, docID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes = append(b.deletes, index+"/"+docID)
	return nil
}

func (b *captureBackend) Search(context.Context, SearchRequest, string, *resource.VersionConfig) (SearchResponse, error) {
	return SearchResponse{}, nil
}

func (b *captureBackend) FederatedSearch(context.Context, FederatedSearchParams) (FederatedSearchResult, error) {
	return FederatedSearchResult{}, nil
}

func (b *captureBackend) allBulkItems() []BulkItem {
	b.mu.Lock()
	defer b.mu.Unlock()
	var all []BulkItem
	for _, call := range b.bulkCalls {
		all = append(all, call...)
	}
	return all
}

func (b *captureBackend) deletesSnapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.deletes...)
}

// rebuildRecordingStore records per-resource lifecycle calls.
type rebuildRecordingStore struct {
	mu       sync.Mutex
	calls    []string
	buildIdx int64
	// driftBudget bounds how many AnyResourceVersionDrifted calls report
	// drift for driftChildren, so a drift-triggered re-build settles instead
	// of looping forever.
	driftBudget   atomic.Int32
	driftChildren map[string]bool
}

func (s *rebuildRecordingStore) record(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, fmt.Sprintf(format, args...))
}

func (s *rebuildRecordingStore) has(prefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

func (s *rebuildRecordingStore) MarkStale(_ context.Context, rs []model.Resource, _ map[string]string) error {
	for _, r := range rs {
		s.record("MarkStale:%s/%s", r.Type, r.Id)
	}
	return nil
}

func (s *rebuildRecordingStore) MarkDeleted(_ context.Context, r model.Resource) (int64, error) {
	s.record("MarkDeleted:%s/%s", r.Type, r.Id)
	return 7, nil
}

func (s *rebuildRecordingStore) BeginBuild(_ context.Context, r model.Resource) (int64, int64, error) {
	s.record("BeginBuild:%s/%s", r.Type, r.Id)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buildIdx++
	return s.buildIdx, 42, nil
}

func (s *rebuildRecordingStore) ClearStale(_ context.Context, r model.Resource, seq int64) error {
	s.record("ClearStale:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}

func (s *rebuildRecordingStore) DeleteResourceIfSeq(_ context.Context, r model.Resource, seq int64) error {
	s.record("DeleteResourceIfSeq:%s/%s:%d", r.Type, r.Id, seq)
	return nil
}

func (s *rebuildRecordingStore) ListStale(context.Context, time.Time, int) ([]StaleResource, error) {
	return nil, nil
}

func (s *rebuildRecordingStore) AddChildResources(_ context.Context, parent model.Resource, _ []model.Resource) error {
	s.record("AddChildResources:%s/%s", parent.Type, parent.Id)
	return nil
}

func (s *rebuildRecordingStore) AddRelations(context.Context, []Relation) error { return nil }

func (s *rebuildRecordingStore) AnyResourceVersionDrifted(_ context.Context, observed []model.VersionedResource) (bool, error) {
	for _, r := range observed {
		if s.driftChildren[r.Id] && s.driftBudget.Add(-1) >= 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *rebuildRecordingStore) GetChildResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}

func (s *rebuildRecordingStore) GetParentResources(context.Context, model.Resource) ([]model.Resource, error) {
	return nil, nil
}

func (s *rebuildRecordingStore) RemoveResource(_ context.Context, r model.Resource) error {
	s.record("RemoveResource:%s/%s", r.Type, r.Id)
	return nil
}

func (s *rebuildRecordingStore) UpsertResource(_ context.Context, r model.Resource, v int64) error {
	s.record("UpsertResource:%s/%s:%d", r.Type, r.Id, v)
	return nil
}

func newRebuildIndexer(st Store, es SearchBackend, plans map[string][]projection.Plan, chunkSize int) *Indexer {
	return mustNew(Config{
		Resources:        twoVersionResources(),
		Plans:            plans,
		ES:               es,
		Store:            st,
		RebuildChunkSize: chunkSize,
	})
}

func TestRebuild_TargetedVersion_WritesOnlySelectedIndex_AndMergesEdges(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	child := model.VersionedResource{Resource: model.Resource{Type: "product", Id: "c1"}, Version: 1}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", child)}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", child)}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", Versions: []int{2}, ResourceIDs: []string{"1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	items := es.allBulkItems()
	if len(items) == 0 {
		t.Fatal("expected a write to product_search_v2")
	}
	for _, it := range items {
		if it.Index != "product_search_v2" {
			t.Fatalf("a targeted rebuild must only write the selected version's index, wrote %s", it.Index)
		}
	}
	if st.has("RemoveResource:product/1") {
		t.Fatal("a targeted rebuild must merge edges, not wipe them: the non-targeted versions' plans did not run, so wiping would drop the edges only they discover")
	}
	if !st.has("AddChildResources:product/1") {
		t.Fatal("edges discovered by the executed plan must still be persisted")
	}
	if !st.has("ClearStale:product/1") {
		t.Fatal("a fully flushed resource must clear its stale mark")
	}
}

func TestRebuild_AllVersions_WipesEdges_AndWritesEveryIndex(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", ResourceIDs: []string{"1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	indices := map[string]bool{}
	for _, it := range es.allBulkItems() {
		indices[it.Index] = true
	}
	if !indices["product_search_v1"] || !indices["product_search_v2"] {
		t.Fatalf("a full rebuild must write every schema version's index, wrote %v", indices)
	}
	if !st.has("RemoveResource:product/1") {
		t.Fatal("a full rebuild must wipe-and-replace edges (ADR 0002)")
	}
	if !st.has("ClearStale:product/1") {
		t.Fatal("a fully flushed resource must clear its stale mark")
	}
}

func TestRebuild_VersionWithoutPlanFails(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	// Config declares v2 but the embedder registered no plan for it.
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", Versions: []int{2}, ResourceIDs: []string{"1"}},
	})
	if err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("rebuilding a version that has no plan must fail loudly, got %v", err)
	}
}

func TestRebuildAll_FlushesInBoundedChunks(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	docs := make([]projection.BuildDoc, 10)
	for i := range docs {
		docs[i] = productDoc(fmt.Sprintf("%d", i+1))
	}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: docs}},
	}}
	idx := newRebuildIndexer(st, es, plans, 4)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err != nil {
		t.Fatal(err)
	}

	if len(es.bulkCalls) < 3 {
		t.Fatalf("10 docs at chunk size 4 must flush in at least 3 bulk requests, got %d", len(es.bulkCalls))
	}
	total := 0
	for _, call := range es.bulkCalls {
		if len(call) > 4 {
			t.Fatalf("a bulk request must not exceed the chunk size: %d items", len(call))
		}
		total += len(call)
	}
	if total != 10 {
		t.Fatalf("every document must be written exactly once, wrote %d", total)
	}
}

func TestRebuildAll_RejectedDocIsMarkedStale_NotCleared(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{rejectIDs: map[string]bool{"3": true}}
	docs := make([]projection.BuildDoc, 5)
	for i := range docs {
		docs[i] = productDoc(fmt.Sprintf("%d", i+1))
	}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: docs}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err == nil {
		t.Fatal("a rebuild with rejected documents must not report success")
	}

	if !st.has("MarkStale:product/3") {
		t.Fatal("a rejected resource must be durably marked stale so the sweep recovers it")
	}
	if st.has("ClearStale:product/3") {
		t.Fatal("a rejected resource must not clear its stale mark")
	}
	for _, id := range []string{"1", "2", "4", "5"} {
		if !st.has("ClearStale:product/" + id) {
			t.Fatalf("resource %s flushed fine and must be cleared", id)
		}
	}
}

func TestRebuildAll_NilDocDeletesFromAllVersions(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	gone := projection.BuildDoc{Root: model.Resource{Type: "product", Id: "2"}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1"), gone, productDoc("3")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err != nil {
		t.Fatal(err)
	}

	deletes := map[string]bool{}
	for _, d := range es.deletesSnapshot() {
		deletes[d] = true
	}
	if !deletes["product_search_v1/2"] || !deletes["product_search_v2/2"] {
		t.Fatalf("a resource gone at source must be deleted from every schema version's index, got %v", deletes)
	}
	for _, it := range es.allBulkItems() {
		if it.ID == "2" {
			t.Fatal("a nil document must never be bulk-written")
		}
	}
	if st.has("ClearStale:product/2") {
		t.Fatal("the delete path owns the deleted resource; the rebuild must not clear it")
	}
	for _, id := range []string{"1", "3"} {
		if !st.has("ClearStale:product/" + id) {
			t.Fatalf("resource %s must still complete normally", id)
		}
	}
}

func TestRebuildAll_ChildDrift_RemarksResourceStale(t *testing.T) {
	st := &rebuildRecordingStore{driftChildren: map[string]bool{"cX": true}}
	st.driftBudget.Store(2)
	es := &captureBackend{}
	child := model.VersionedResource{Resource: model.Resource{Type: "product", Id: "cX"}, Version: 5}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1", child), productDoc("2")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.WaitForIdle(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !st.has("MarkStale:product/1") {
		t.Fatal("a child that drifted during the rebuild's edge-less window must re-mark the parent (ADR 0002 drift check)")
	}
	if st.has("MarkStale:product/2") {
		t.Fatal("a resource without drifted children must not be re-marked")
	}
}

// failingExecuter fails the plan execution with the given error.
type failingExecuter struct{ err error }

func (e *failingExecuter) Execute(context.Context, projection.BuildRequest) <-chan aggregation.ExecutionResult[projection.BuildDoc] {
	ch := make(chan aggregation.ExecutionResult[projection.BuildDoc], 1)
	ch <- aggregation.ExecutionResult[projection.BuildDoc]{Err: e.err}
	close(ch)
	return ch
}

func TestRebuildByIDs_PlanErrorLeavesResourceStale(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &failingExecuter{err: errors.New("provider exploded")}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{
		{ResourceType: "product", ResourceIDs: []string{"1"}},
	})
	if err == nil {
		t.Fatal("a rebuild that failed a resource must not report success")
	}

	if !st.has("MarkStale:product/1") {
		t.Fatal("a resource whose plan failed must be durably marked stale so the sweep recovers it")
	}
	if st.has("ClearStale:product/1") {
		t.Fatal("a failed resource must not clear its stale mark")
	}
	for _, it := range es.allBulkItems() {
		if it.ID == "1" {
			t.Fatal("no partial version set may be written for a resource whose plan failed")
		}
	}
}

func TestRebuildAll_PageError_AbortsAndMarksBegunResourcesStale(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &pagingExecuter{pages: []aggregation.ExecutionResult[projection.BuildDoc]{
			{Items: []projection.BuildDoc{productDoc("1"), productDoc("2")}},
			{Err: errors.New("provider page exploded")},
		}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.RebuildNow(context.Background(), []ResourceSelector{{ResourceType: "product"}})
	if err == nil || !strings.Contains(err.Error(), "provider page exploded") {
		t.Fatalf("a page error must abort the walk, got %v", err)
	}

	for _, id := range []string{"1", "2"} {
		if !st.has("MarkStale:product/" + id) {
			t.Fatalf("resource %s had its edges wiped but was never completed — it must be marked stale so the sweep repairs it", id)
		}
		if st.has("ClearStale:product/" + id) {
			t.Fatalf("resource %s must not be cleared on abort", id)
		}
	}
}

func TestBuild_UpsertConflict_IsBenignAndBuildCompletes(t *testing.T) {
	st := &rebuildRecordingStore{}
	es := &captureBackend{upsertConflict: map[string]bool{"product_search_v1/1": true}}
	plans := map[string][]projection.Plan{"product": {
		{Version: 1, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
		{Version: 2, Executer: &staticExecuter{docs: []projection.BuildDoc{productDoc("1")}}},
	}}
	idx := newRebuildIndexer(st, es, plans, 0)

	err := idx.Build(context.Background(), BuildArgs{ResourceType: "product", ResourceIds: []string{"1"}})
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, u := range es.upserts {
		if u == "product_search_v2/1" {
			found = true
		}
	}
	if !found {
		t.Fatal("an OCC loss on one version's index must not stop the build from writing the remaining versions")
	}
	if !st.has("ClearStale:product/1:42") {
		t.Fatal("an OCC loss is benign — the seq-guarded clear must still run (a newer change keeps the mark via the guard)")
	}
}
