package store

import (
	"sync"
	"time"
)

type AProj struct {
	TenantID string
	AID      string
	Status   string
	BID      string
	CIDs     map[string]struct{}
	Updated  time.Time
}

type BProj struct {
	TenantID string
	BID      string
	Name     string
	Updated  time.Time
}

type CProj struct {
	TenantID string
	CID      string
	Type     string
	State    string
	Updated  time.Time
}

type Store struct {
	mu sync.RWMutex

	// projections
	a map[string]*AProj // key: tenant|a_id
	b map[string]*BProj // key: tenant|b_id
	c map[string]*CProj // key: tenant|c_id

	// reverse relations (for fan-out reindex)
	bToAs map[string]map[string]struct{} // key: tenant|b_id -> set(tenant|a_id)
	cToAs map[string]map[string]struct{} // key: tenant|c_id -> set(tenant|a_id)

	// naive event de-dup (in-memory TTL)
	seenEvent map[string]time.Time
}

func New() *Store {
	return &Store{
		a:         map[string]*AProj{},
		b:         map[string]*BProj{},
		c:         map[string]*CProj{},
		bToAs:     map[string]map[string]struct{}{},
		cToAs:     map[string]map[string]struct{}{},
		seenEvent: map[string]time.Time{},
	}
}

func Key(tenant, id string) string { return tenant + "|" + id }

func (s *Store) SeenRecently(eventID string, ttl time.Duration) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	// cleanup occasionally (cheap)
	for k, t := range s.seenEvent {
		if now.Sub(t) > ttl {
			delete(s.seenEvent, k)
		}
	}

	if t, ok := s.seenEvent[eventID]; ok && now.Sub(t) <= ttl {
		return true
	}
	s.seenEvent[eventID] = now
	return false
}

type RelUpdates struct {
	AKey                string
	PrevBKey, NewBKey   string
	PrevCKeys, NewCKeys []string
	BKeysToRefresh      []string
	CKeysToRefresh      []string
	AsAffectedByBChange []string
	AsAffectedByCChange []string
}

func (s *Store) UpsertA(tenant, aID, status, bID string, cIDs []string) (bKeysToRefresh, cKeysToRefresh []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	aKey := Key(tenant, aID)
	prev := s.a[aKey]

	newC := map[string]struct{}{}
	for _, cid := range cIDs {
		if cid == "" {
			continue
		}
		newC[Key(tenant, cid)] = struct{}{}
	}

	var prevBKey string
	prevC := map[string]struct{}{}
	if prev != nil {
		prevBKey = Key(tenant, prev.BID)
		for k := range prev.CIDs {
			prevC[k] = struct{}{}
		}
	}

	newBKey := ""
	if bID != "" {
		newBKey = Key(tenant, bID)
	}

	// update A projection
	s.a[aKey] = &AProj{
		TenantID: tenant,
		AID:      aID,
		Status:   status,
		BID:      bID,
		CIDs:     newC,
		Updated:  time.Now(),
	}

	// update bToAs
	if prevBKey != "" && prevBKey != newBKey {
		if set := s.bToAs[prevBKey]; set != nil {
			delete(set, aKey)
		}
		bKeysToRefresh = append(bKeysToRefresh, prevBKey)
	}
	if newBKey != "" {
		set := s.bToAs[newBKey]
		if set == nil {
			set = map[string]struct{}{}
			s.bToAs[newBKey] = set
		}
		set[aKey] = struct{}{}
		bKeysToRefresh = append(bKeysToRefresh, newBKey)
	}

	// update cToAs (diff)
	for k := range prevC {
		if _, ok := newC[k]; !ok {
			if set := s.cToAs[k]; set != nil {
				delete(set, aKey)
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
			set[aKey] = struct{}{}
			cKeysToRefresh = append(cKeysToRefresh, k)
		}
	}

	return uniq(bKeysToRefresh), uniq(cKeysToRefresh)
}

func (s *Store) DeleteA(tenant, aID string) (bKeysToRefresh, cKeysToRefresh []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	aKey := Key(tenant, aID)
	prev := s.a[aKey]
	if prev == nil {
		return nil, nil
	}

	if prev.BID != "" {
		bKey := Key(tenant, prev.BID)
		if set := s.bToAs[bKey]; set != nil {
			delete(set, aKey)
		}
		bKeysToRefresh = append(bKeysToRefresh, bKey)
	}

	for cKey := range prev.CIDs {
		if set := s.cToAs[cKey]; set != nil {
			delete(set, aKey)
		}
		cKeysToRefresh = append(cKeysToRefresh, cKey)
	}

	delete(s.a, aKey)
	return uniq(bKeysToRefresh), uniq(cKeysToRefresh)
}

func (s *Store) UpsertB(tenant, bID, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b[Key(tenant, bID)] = &BProj{TenantID: tenant, BID: bID, Name: name, Updated: time.Now()}
}

func (s *Store) DeleteB(tenant, bID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.b, Key(tenant, bID))
}

func (s *Store) UpsertC(tenant, cID, typ, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.c[Key(tenant, cID)] = &CProj{TenantID: tenant, CID: cID, Type: typ, State: state, Updated: time.Now()}
}

func (s *Store) DeleteC(tenant, cID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.c, Key(tenant, cID))
}

func (s *Store) AffectedAsByB(tenant, bID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bKey := Key(tenant, bID)
	set := s.bToAs[bKey]
	out := make([]string, 0, len(set))
	for aKey := range set {
		out = append(out, aKey)
	}
	return out
}

func (s *Store) AffectedAsByC(tenant, cID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cKey := Key(tenant, cID)
	set := s.cToAs[cKey]
	out := make([]string, 0, len(set))
	for aKey := range set {
		out = append(out, aKey)
	}
	return out
}

func (s *Store) SnapshotA(aKey string) *AProj {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p := s.a[aKey]; p != nil {
		cp := *p
		cp.CIDs = map[string]struct{}{}
		for k := range p.CIDs {
			cp.CIDs[k] = struct{}{}
		}
		return &cp
	}
	return nil
}

func (s *Store) SnapshotB(bKey string) *BProj {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p := s.b[bKey]; p != nil {
		cp := *p
		return &cp
	}
	return nil
}

func (s *Store) SnapshotC(cKey string) *CProj {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p := s.c[cKey]; p != nil {
		cp := *p
		return &cp
	}
	return nil
}

func (s *Store) CountAsForB(bKey string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.bToAs[bKey])
}

func (s *Store) CountAsForC(cKey string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cToAs[cKey])
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
