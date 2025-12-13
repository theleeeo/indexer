package store

import (
	"indexer/gen/index/v1"
	"sync"
	"time"
)

type AProj struct {
	AID     string
	Status  string
	BID     string
	CIDs    map[string]struct{}
	Updated time.Time
}

type BProj struct {
	BID     string
	Name    string
	Updated time.Time
}

type CProj struct {
	CID     string
	Type    string
	State   string
	Updated time.Time
}

type ResourceKey string

func Key(resource, resourceID string) ResourceKey {
	return ResourceKey(resource + "|" + resourceID)
}

func KeyParts(rk ResourceKey) (string, string) {
	parts := string(rk)
	for i := 0; i < len(parts); i++ {
		if parts[i] == '|' {
			return parts[:i], parts[i+1:]
		}
	}
	return parts, ""
}

type Store struct {
	mu sync.RWMutex

	// "resource|resource_id"
	resources map[ResourceKey]struct{}
	// child: parents
	relations map[ResourceKey][]ResourceKey

	// projections
	a map[string]*AProj
	b map[string]*BProj
	c map[string]*CProj

	// reverse relations (for fan-out reindex)
	bToAs map[string]map[string]struct{}
	cToAs map[string]map[string]struct{}
}

func New() *Store {
	return &Store{
		resources: map[ResourceKey]struct{}{},
		relations: map[ResourceKey][]ResourceKey{},

		a:     map[string]*AProj{},
		b:     map[string]*BProj{},
		c:     map[string]*CProj{},
		bToAs: map[string]map[string]struct{}{},
		cToAs: map[string]map[string]struct{}{},
	}
}

func (s *Store) StoreRelations(resource, resourceId string, relations []*index.Relation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parentResourceKey := Key(resource, resourceId)
	s.resources[parentResourceKey] = struct{}{}

	for _, rl := range relations {
		childResourceKey := Key(rl.Resource, rl.ResourceId)
		s.relations[childResourceKey] = append(s.relations[childResourceKey], parentResourceKey)
	}
}

type RelationChangesParameter struct {
	// If SetResource is set, we are setting a relation to this. Only for kind: one
	SetRelation *index.Relation

	// For kind: many relations, we can have multiple additions/removals
	AddRelations    []*index.Relation
	RemoveRelations []*index.Relation
}

func (s *Store) UpdateRelations(resource, resourceId string, relChanges RelationChangesParameter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rk := Key(resource, resourceId)
	_, exists := s.resources[rk]
	if !exists {
		s.resources[rk] = struct{}{}
	}

	if relChanges.SetRelation != nil {
		relatedRK := Key(relChanges.SetRelation.Resource, relChanges.SetRelation.ResourceId)

		relations := s.relations[rk]
		newRelations := make([]ResourceKey, 0, len(relations))
		for _, r := range relations {
			if r != relatedRK {
				newRelations = append(newRelations, r)
			}
		}
		newRelations = append(newRelations, relatedRK)
		s.relations[rk] = newRelations
	} else {
		for _, rel := range relChanges.AddRelations {
			relatedRK := Key(rel.Resource, rel.ResourceId)
			s.relations[rk] = append(s.relations[rk], relatedRK)
		}
		for _, rel := range relChanges.RemoveRelations {
			relatedRK := Key(rel.Resource, rel.ResourceId)
			relations := s.relations[rk]
			newRelations := make([]ResourceKey, 0, len(relations))
			for _, r := range relations {
				if r != relatedRK {
					newRelations = append(newRelations, r)
				}
			}
			s.relations[rk] = newRelations
		}
	}
}

func (s *Store) GetRelatedResources(resource, resourceID string) []ResourceKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rk := Key(resource, resourceID)
	return s.relations[rk]
}

func (s *Store) UpsertA(aID, status, bID string, cIDs []string) (bKeysToRefresh, cKeysToRefresh []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.a[aID]

	newC := map[string]struct{}{}
	for _, cid := range cIDs {
		if cid == "" {
			continue
		}
		newC[cid] = struct{}{}
	}

	var prevBKey string
	prevC := map[string]struct{}{}
	if prev != nil {
		prevBKey = prev.BID
		for k := range prev.CIDs {
			prevC[k] = struct{}{}
		}
	}

	// update A projection
	s.a[aID] = &AProj{
		AID:     aID,
		Status:  status,
		BID:     bID,
		CIDs:    newC,
		Updated: time.Now(),
	}

	// update bToAs
	if prevBKey != "" && prevBKey != bID {
		if set := s.bToAs[prevBKey]; set != nil {
			delete(set, aID)
		}
		bKeysToRefresh = append(bKeysToRefresh, prevBKey)
	}
	if bID != "" {
		set := s.bToAs[bID]
		if set == nil {
			set = map[string]struct{}{}
			s.bToAs[bID] = set
		}
		set[aID] = struct{}{}
		bKeysToRefresh = append(bKeysToRefresh, bID)
	}

	// update cToAs (diff)
	for k := range prevC {
		if _, ok := newC[k]; !ok {
			if set := s.cToAs[k]; set != nil {
				delete(set, aID)
			}
			cKeysToRefresh = append(cKeysToRefresh, k)
		}
	}
	for k := range newC {
		if _, ok := prevC[k]; !ok {
			set := s.cToAs[k]
			if set == nil {
				set = map[string]struct{}{}
				s.cToAs[k] = set
			}
			set[aID] = struct{}{}
			cKeysToRefresh = append(cKeysToRefresh, k)
		}
	}

	return uniq(bKeysToRefresh), uniq(cKeysToRefresh)
}

func (s *Store) DeleteA(aID string) (bKeysToRefresh, cKeysToRefresh []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.a[aID]
	if prev == nil {
		return nil, nil
	}

	if prev.BID != "" {
		if set := s.bToAs[prev.BID]; set != nil {
			delete(set, aID)
		}
		bKeysToRefresh = append(bKeysToRefresh, prev.BID)
	}

	for cKey := range prev.CIDs {
		if set := s.cToAs[cKey]; set != nil {
			delete(set, aID)
		}
		cKeysToRefresh = append(cKeysToRefresh, cKey)
	}

	delete(s.a, aID)
	return uniq(bKeysToRefresh), uniq(cKeysToRefresh)
}

func (s *Store) UpsertB(bID, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b[bID] = &BProj{BID: bID, Name: name, Updated: time.Now()}
}

func (s *Store) DeleteB(bID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.b, bID)
}

func (s *Store) UpsertC(cID, typ, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.c[cID] = &CProj{CID: cID, Type: typ, State: state, Updated: time.Now()}
}

func (s *Store) DeleteC(cID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.c, cID)
}

func (s *Store) AffectedAsByB(bID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	set := s.bToAs[bID]
	out := make([]string, 0, len(set))
	for aKey := range set {
		out = append(out, aKey)
	}
	return out
}

func (s *Store) AffectedAsByC(cID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	set := s.cToAs[cID]
	out := make([]string, 0, len(set))
	for aKey := range set {
		out = append(out, aKey)
	}
	return out
}

func (s *Store) SnapshotA(aID string) *AProj {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p := s.a[aID]; p != nil {
		cp := *p
		cp.CIDs = map[string]struct{}{}
		for k := range p.CIDs {
			cp.CIDs[k] = struct{}{}
		}
		return &cp
	}
	return nil
}

func (s *Store) SnapshotB(bID string) *BProj {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p := s.b[bID]; p != nil {
		cp := *p
		return &cp
	}
	return nil
}

func (s *Store) SnapshotC(cID string) *CProj {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p := s.c[cID]; p != nil {
		cp := *p
		return &cp
	}
	return nil
}

func (s *Store) CountAsForB(bID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.bToAs[bID])
}

func (s *Store) CountAsForC(cID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cToAs[cID])
}

func uniq(in []string) []string {
	m := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" {
			continue
		}
		if _, ok := m[v]; ok {
			continue
		}
		m[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
