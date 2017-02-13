package kv

import (
	"github.com/movio/kasper"
)

// InMemoryKeyValueStore is a key-value storage that stores data in memory using map
type InMemoryKeyValueStore struct {
	witness *kasper.StructPtrWitness
	m map[string]interface{}
}

// NewInMemoryKeyValueStore creates new store
func NewInMemoryKeyValueStore(size int, structPtr interface{}) *InMemoryKeyValueStore {

	return &InMemoryKeyValueStore{
		witness: kasper.NewStructPtrWitness(structPtr),
		m: make(map[string]interface{}, size),
	}
}

// Get gets data by key from store and populates value
func (s *InMemoryKeyValueStore) Get(key string) (interface{}, error) {
	src, found := s.m[key]
	if !found {
		return false, nil
	}
	return src, nil
}

// Put updates key in store with serialized value
func (s *InMemoryKeyValueStore) Put(key string, value interface{}) error {
	s.witness.Assert(value)
	s.m[key] = value
	return nil
}

// Delete removes key from store
func (s *InMemoryKeyValueStore) Delete(key string) error {
	delete(s.m, key)
	return nil
}
