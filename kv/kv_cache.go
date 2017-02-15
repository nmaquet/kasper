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
	if len(s.cache) > s.maxItemCount {
		s.Flush()
	}
}

func (s *CachedKeyValueStore) Delete(key string) error {
	delete(s.cache, key)
	return s.child.Delete(key)
}

func (s *CachedKeyValueStore) Flush() error {
	for k, v := range s.cache {
		err := s.child.Put(k, v)
		if err != nil {
			return err
		}
	}
	s.cache = make(map[string]interface{})
	return nil
}
