package store

import (
	"indexer/gen/index/v1"
	"sync"
)

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
}

func New() *Store {
	return &Store{
		resources: map[ResourceKey]struct{}{},
		relations: map[ResourceKey][]ResourceKey{},
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
