package kv

import (
	"github.com/movio/kasper/util"
)

type CachedKeyValueStore struct {
	witness      *util.StructPtrWitness
	child        KeyValueStore
	cache        map[string]interface{}
	maxItemCount int
}

func NewCachedKeyValueStore(child KeyValueStore, structPtr interface{}, maxItemCount int) *CachedKeyValueStore {
	return &CachedKeyValueStore{
		util.NewStructPtrWitness(structPtr),
		child,
		make(map[string]interface{}),
		maxItemCount,
	}
}

func (s *CachedKeyValueStore) Get(key string) (interface{}, error) {
	value, found := s.cache[key]
	if found {
		return value, nil
	}
	return s.child.Get(key)
}

func (s *CachedKeyValueStore) Put(key string, value interface{}) error {
	s.witness.Assert(value)
	s.cache[key] = value
	if len(s.cache) <= s.maxItemCount {
		return nil
	}
	return s.clearCache()
}

func (s *CachedKeyValueStore) PutAll(entries []*Entry) error {
	for _, entry := range entries {
		err := s.Put(entry.key, entry.value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CachedKeyValueStore) Delete(key string) error {
	delete(s.cache, key)
	return s.child.Delete(key)
}

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
