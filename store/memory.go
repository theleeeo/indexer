package store

import (
	"context"
	"indexer/model"
	"sync"
)

var _ Store = (*MemoryStore)(nil)

type MemoryStore struct {
	mu sync.RWMutex

	// child: parents
	relations map[model.Resource][]model.Resource
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		relations: map[model.Resource][]model.Resource{},
	}
}

func (s *MemoryStore) AddRelations(_ context.Context, relations []Relation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, rl := range relations {
		s.relations[rl.Child] = append(s.relations[rl.Child], rl.Parent)
	}

	return nil
}

func (s *MemoryStore) RemoveRelation(ctx context.Context, relation Relation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	parents, ok := s.relations[relation.Child]
	if !ok {
		return nil
	}

	newParents := []model.Resource{}
	for _, pr := range parents {
		if pr != relation.Parent {
			newParents = append(newParents, pr)
		}
	}
	s.relations[relation.Child] = newParents
	return nil
}

func (s *MemoryStore) SetRelation(ctx context.Context, relation Relation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.relations[relation.Child] = []model.Resource{relation.Parent}
	return nil
}

func (s *MemoryStore) GetParentResources(_ context.Context, childResource model.Resource) ([]model.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	parents, ok := s.relations[childResource]
	if !ok {
		return []model.Resource{}, nil
	}
	return parents, nil
}

func (s *MemoryStore) GetChildResources(_ context.Context, parentResource model.Resource) ([]model.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var children []model.Resource
	for child, parents := range s.relations {
		for _, pr := range parents {
			if pr == parentResource {
				children = append(children, child)
				break
			}
		}
	}
	return children, nil
}

func (s *MemoryStore) RemoveResource(ctx context.Context, resource model.Resource) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.relations, resource)
	return nil
}
