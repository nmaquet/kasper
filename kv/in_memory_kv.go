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

// TBD
func (s *InMemoryKeyValueStore) GetAll(keys []string) ([]*Entry, error) {
	entries := make([]*Entry, len(keys))
	for i, key := range keys {
		value, err := s.Get(key)
		if err != nil {
			return nil, err
		}
		entries[i] = &Entry{ key, value }
	}
	return entries, nil
}

// Put updates key in store with serialized value
func (s *InMemoryKeyValueStore) Put(key string, value interface{}) error {
	s.witness.Assert(value)
	s.m[key] = value
	return nil
}

// PutAll bulk executes all Put operations
func (s *InMemoryKeyValueStore) PutAll(entries []*Entry) error {
	for _, entry := range entries {
		err := s.Put(entry.Key, entry.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete removes key from store
func (s *InMemoryKeyValueStore) Delete(key string) error {
	delete(s.m, key)
	return nil
}

// Flush does nothing for in memory storage
func (s *InMemoryKeyValueStore) Flush() error {
	return nil
}
