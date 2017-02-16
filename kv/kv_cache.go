package kv

import (
	"github.com/movio/kasper/util"
)

// CachedKeyValueStore provides cazching for child store.
type CachedKeyValueStore struct {
	witness      *util.StructPtrWitness
	child        KeyValueStore
	cache        map[string]interface{}
	maxItemCount int
}

// NewCachedKeyValueStore creates new CachedKeyValueStore.
// Child is a store to wrap, for example, ElasticsearchKeyValueStore
// StructPtr - witness, value struct of child storage
// maxItemCount - cache is flushed to store automatically if amount of keys in cache bigger than maxItemCount
func NewCachedKeyValueStore(child KeyValueStore, structPtr interface{}, maxItemCount int) *CachedKeyValueStore {
	return &CachedKeyValueStore{
		util.NewStructPtrWitness(structPtr),
		child,
		make(map[string]interface{}),
		maxItemCount,
	}
}

// Get returns key for cache. If key is not found in cache, it is retrieved from child store
func (s *CachedKeyValueStore) Get(key string) (interface{}, error) {
	value, found := s.cache[key]
	if found {
		return value, nil
	}
	return s.child.Get(key)
}

// Put insert new key-value pair to cache. If number of keys in cache more than maxItemCount,
// cache is flushed to child store and cleared.
func (s *CachedKeyValueStore) Put(key string, value interface{}) error {
	s.witness.Assert(value)
	s.cache[key] = value
	if len(s.cache) <= s.maxItemCount {
		return nil
	}
	return s.clearCache()
}

// PutAll bulk executes Put operation for several entries
func (s *CachedKeyValueStore) PutAll(entries []*Entry) error {
	for _, entry := range entries {
		err := s.Put(entry.key, entry.value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete removes key both from cache and child store
func (s *CachedKeyValueStore) Delete(key string) error {
	delete(s.cache, key)
	return s.child.Delete(key)
}

// Flush flushes cache to child store
func (s *CachedKeyValueStore) Flush() error {
	err := s.clearCache()
	if err != nil {
		return err
	}
	return s.child.Flush()
}

func (s *CachedKeyValueStore) clearCache() error {
	entries := make([]*Entry, len(s.cache))
	i := 0
	for key, value := range s.cache {
		entries[i] = &Entry{key, value}
		i++
	}
	err := s.child.PutAll(entries)
	if err != nil {
		return err
	}
	s.cache = make(map[string]interface{})
	return nil
}
