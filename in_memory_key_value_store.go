package kasper

import (
	"reflect"
	"log"
)

type InMemoryKeyValueStore struct {
	m map[string]StoreValue
}

func NewInMemoryKeyValueStore(size int) *InMemoryKeyValueStore {
	return &InMemoryKeyValueStore{
		m: make(map[string]StoreValue, size),
	}
}

func (s*InMemoryKeyValueStore) Get(key string, dst StoreValue) (bool, error) {
	src, found := s.m[key]
	if !found {
		return false, nil
	}
	x := reflect.ValueOf(src)
	if x.Kind() != reflect.Ptr {
		log.Fatal("InMemoryKeyValueStore.Get() must receive a pointer value as second argument")
	}
	starX := x.Elem()
	y := reflect.New(starX.Type())
	starY := y.Elem()
	starY.Set(starX)
	reflect.ValueOf(dst).Elem().Set(y.Elem())
	return true, nil
}

func (s*InMemoryKeyValueStore) Put(key string, value StoreValue) error {
	s.m[key] = value
	return nil
}

func (s*InMemoryKeyValueStore) Delete(key string) error {
	delete(s.m, key)
	return nil
}