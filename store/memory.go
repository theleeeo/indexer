package store

import (
	"context"
	"indexer/gen/index/v1"
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

func (s *MemoryStore) AddRelations(_ context.Context, resource Resource, relations []*index.Relation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, rl := range relations {
		childResource := Resource{Type: rl.Resource, Id: rl.ResourceId}
		s.relations[childResource] = append(s.relations[childResource], resource)
	}

	return nil
}

func (s *MemoryStore) RemoveRelation(ctx context.Context, resource, relResource Resource) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	parents, ok := s.relations[relResource]
	if !ok {
		return nil
	}

	newParents := []Resource{}
	for _, pr := range parents {
		if pr != resource {
			newParents = append(newParents, pr)
		}
	}
	s.relations[relResource] = newParents
	return nil
}

func (s *MemoryStore) SetRelation(ctx context.Context, parentResource Resource, relatedResource Resource) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.relations[relatedResource] = []Resource{parentResource}
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
