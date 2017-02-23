package kasper

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

type Planet struct {
	name string
}

func newTestInMemoryKV() *InMemoryKeyValueStore {
	s := &InMemoryKeyValueStore{
		witness:         newStructPtrWitness(&Planet{}),
		m:               make(map[string]interface{}, 10),
		metricsProvider: &NoopMetricsProvider{},
	}
	s.createMetrics()
	return s
}

func TestInMemoryKeyValueStore_Get_ExistingKey(t *testing.T) {
	s := newTestInMemoryKV()
	expected := &Planet{"Mars"}
	s.m["mars"] = expected
	actual, err := s.Get("mars")
	assert.Equal(t, nil, err)
	assert.Equal(t, expected, actual)
}

func TestInMemoryKeyValueStore_Get_MissingKey(t *testing.T) {
	s := newTestInMemoryKV()
	expected := &Planet{"Mars"}
	s.m["mars"] = expected
	actual, err := s.Get("jupiter")
	assert.Equal(t, nil, err)
	assert.Equal(t, (*Planet)(nil), actual)
}

func TestInMemoryKeyValueStore_Put_Normal(t *testing.T) {
	s := newTestInMemoryKV()
	earth := &Planet{"Earth"}
	s.Put("earth", earth)
	assert.Equal(t, earth, s.m["earth"])
}

func TestInMemoryKeyValueStore_Put_Overwrite(t *testing.T) {
	s := newTestInMemoryKV()
	s.m["earth"] = &Planet{"Gaia"}
	earth := &Planet{"Earth"}
	s.Put("earth", earth)
	assert.Equal(t, earth, s.m["earth"])
}

func TestInMemoryKeyValueStore_Delete_Existing(t *testing.T) {
	s := newTestInMemoryKV()
	s.m["earth"] = &Planet{"Earth"}
	s.Delete("earth")
	assert.Equal(t, 0, len(s.m))
}

func TestInMemoryKeyValueStore_Delete_NonExisting(t *testing.T) {
	s := newTestInMemoryKV()
	s.m["earth"] = &Planet{"Earth"}
	s.Delete("mercury")
	assert.Equal(t, 1, len(s.m))
}

func TestInMemoryKeyValueStore_GetAll(t *testing.T) {
	s := newTestInMemoryKV()
	earth := &Planet{"Earth"}
	s.m["earth"] = earth
	entries, err := s.GetAll([]string{"mercury", "earth"})
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(entries))
	assert.Equal(t, entries[0].Key, "mercury")
	assert.Nil(t, entries[0].Value)
	assert.Equal(t, entries[1].Key, "earth")
	assert.Equal(t, entries[1].Value, earth)
}

func TestInMemoryKeyValueStore_PutAll(t *testing.T) {
	s := newTestInMemoryKV()
	earth := &Planet{"Earth"}
	mars := &Planet{"Mars"}
	err := s.PutAll([]*Entry{{"earth", earth}, {"mars", mars}})
	assert.Nil(t, err)
	assert.Equal(t, len(s.m), 2)
	assert.Equal(t, s.m["earth"], earth)
	assert.Equal(t, s.m["mars"], mars)
}

func TestInMemoryKeyValueStore_Flush(t *testing.T) {
	s := newTestInMemoryKV()
	err := s.Flush()
	assert.Nil(t, err)
}

func BenchmarkInMemoryKeyValueStore_Get(b *testing.B) {
	s := newTestInMemoryKV()
	s.m["earth"] = &Planet{"Earth"}
	s.m["mars"] = &Planet{"Mars"}
	s.m["jupiter"] = &Planet{"Jupiter"}
	s.m["saturn"] = &Planet{"Saturn"}
	keys := []string{"earth", "mars", "jupiter", "saturn", "mercury", "venus", "neptune"}
	keysLength := len(keys)
	for i := 0; i < b.N; i++ {
		s.Get(keys[i%keysLength])
	}
}

func BenchmarkInMemoryKeyValueStore_Put(b *testing.B) {
	s := newTestInMemoryKV()
	entries := []*Entry{
		{"earth", &Planet{"Earth"}},
		{"mars", &Planet{"Mars"}},
		{"jupiter", &Planet{"Jupiter"}},
		{"saturn", &Planet{"Saturn"}},
		{"mercury", &Planet{"Mercury"}},
		{"venus", &Planet{"Venus"}},
		{"neptune", &Planet{"Neptune"}},
	}
	entriesLength := len(entries)
	for i := 0; i < b.N; i++ {
		entry := entries[i%entriesLength]
		s.Put(entry.Key, entry.Value)
	}
}
