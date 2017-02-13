package kv

import (
	"github.com/movio/kasper/util"
)

// InMemoryKeyValueStore is a key-value storage that stores data in memory using map
type InMemoryKeyValueStore struct {
	witness *util.StructPtrWitness
	m       map[string]interface{}
}

// NewInMemoryKeyValueStore creates new store.
// StructPtr should be a pointer to struct type that is used
// for serialization and deserialization of store values.
func NewInMemoryKeyValueStore(size int, structPtr interface{}) *InMemoryKeyValueStore {
	return &InMemoryKeyValueStore{
		witness: util.NewStructPtrWitness(structPtr),
		m:       make(map[string]interface{}, size),
	}
}

// Get gets value by key from store
func (s *InMemoryKeyValueStore) Get(key string) (interface{}, error) {
	src, found := s.m[key]
	if !found {
		return s.witness.Nil(), nil
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
