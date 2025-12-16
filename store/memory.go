package store

import (
	"context"
	"sync"
)

var _ Store = (*MemoryStore)(nil)

type MemoryStore struct {
	mu sync.RWMutex

	// child: parents
	relations map[Resource][]Resource
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		relations: map[Resource][]Resource{},
	}
}

func (s *MemoryStore) AddRelations(_ context.Context, relations []Relation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, rl := range relations {
		s.relations[rl.Children] = append(s.relations[rl.Children], rl.Parent)
	}

	return nil
}

func (s *MemoryStore) RemoveRelation(ctx context.Context, relation Relation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	parents, ok := s.relations[relation.Children]
	if !ok {
		return nil
	}

	newParents := []Resource{}
	for _, pr := range parents {
		if pr != relation.Parent {
			newParents = append(newParents, pr)
		}
	}
	s.relations[relation.Children] = newParents
	return nil
}

func (s *MemoryStore) SetRelation(ctx context.Context, relation Relation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.relations[relation.Children] = []Resource{relation.Parent}
	return nil
}

func (s *MemoryStore) GetParentResources(_ context.Context, childResource Resource) ([]Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	parents, ok := s.relations[childResource]
	if !ok {
		return []Resource{}, nil
	}
	return parents, nil
}

func (s *MemoryStore) RemoveResource(ctx context.Context, resource Resource) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.relations, resource)
	return nil
}
