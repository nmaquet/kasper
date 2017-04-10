package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var mercury = []byte("mercury")
var venus = []byte("venus")
var earth = []byte("earth")
var mars = []byte("mars")
var jupiter = []byte("jupiter")
var saturn = []byte("saturn")
var uranus = []byte("uranus")
var neptune = []byte("neptune")

func newTestInMemoryKV() *InMemoryKeyValueStore {
	s := &InMemoryKeyValueStore{
		m:               make(map[string][]byte, 10),
		metricsProvider: &NoopMetricsProvider{},
	}
	s.createMetrics()
	return s
}

func TestNewInMemoryKeyValueStore(t *testing.T) {
	NewInMemoryKeyValueStore(10)
}

func TestInMemoryKeyValueStore_Get_ExistingKey(t *testing.T) {
	s := newTestInMemoryKV()
	s.Put("mercury", mercury)
	actual, err := s.Get("mercury")
	assert.Equal(t, nil, err)
	assert.Equal(t, mercury, actual)
}

func TestInMemoryKeyValueStore_Get_MissingKey(t *testing.T) {
	s := newTestInMemoryKV()
	s.Put("earth", earth)
	actual, err := s.Get("venus")
	assert.Nil(t, err)
	assert.Nil(t, actual)
}

func TestInMemoryKeyValueStore_Put_Normal(t *testing.T) {
	s := newTestInMemoryKV()
	err := s.Put("venus", venus)
	assert.Nil(t, err)
	assert.Equal(t, venus, s.m["venus"])
}

func TestInMemoryKeyValueStore_Put_Overwrite(t *testing.T) {
	s := newTestInMemoryKV()
	s.Put("earth", jupiter)
	err := s.Put("earth", earth)
	assert.Nil(t, err)
	assert.Equal(t, earth, s.m["earth"])
}

func TestInMemoryKeyValueStore_Delete_Existing(t *testing.T) {
	s := newTestInMemoryKV()
	s.m["neptune"] = neptune
	err := s.Delete("neptune")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(s.m))
}

func TestInMemoryKeyValueStore_Delete_NonExisting(t *testing.T) {
	s := newTestInMemoryKV()
	s.m["uranus"] = uranus
	err := s.Delete("mercury")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(s.m))
}

func TestInMemoryKeyValueStore_GetAll(t *testing.T) {
	s := newTestInMemoryKV()
	s.m["mars"] = mars
	kvs, err := s.GetAll([]string{"venus", "mars"})
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(kvs))
	assert.Equal(t, kvs[0].Key, "venus")
	assert.Nil(t, kvs[0].Value)
	assert.Equal(t, kvs[1].Key, "mars")
	assert.Equal(t, kvs[1].Value, mars)
}

func TestInMemoryKeyValueStore_GetAll_Empty(t *testing.T) {
	s := newTestInMemoryKV()
	_, err := s.GetAll([]string{})
	assert.Nil(t, err)
	_, err = s.GetAll(nil)
	assert.Nil(t, err)
}

func TestInMemoryKeyValueStore_PutAll(t *testing.T) {
	s := newTestInMemoryKV()
	err := s.PutAll([]KeyValue{{"jupiter", jupiter}, {"neptune", neptune}})
	assert.Nil(t, err)
	assert.Equal(t, len(s.m), 2)
	assert.Equal(t, s.m["jupiter"], jupiter)
	assert.Equal(t, s.m["neptune"], neptune)
}

func TestInMemoryKeyValueStore_PutAll_Empty(t *testing.T) {
	s := newTestInMemoryKV()
	err := s.PutAll([]KeyValue{})
	assert.Nil(t, err)
	err = s.PutAll(nil)
	assert.Nil(t, err)
}

func TestInMemoryKeyValueStore_Flush(t *testing.T) {
	s := newTestInMemoryKV()
	err := s.Flush()
	assert.Nil(t, err)
}

func BenchmarkInMemoryKeyValueStore_Get(b *testing.B) {
	s := newTestInMemoryKV()
	s.m["earth"] = earth
	s.m["jupiter"] = jupiter
	s.m["mercury"] = mercury
	s.m["neptune"] = neptune
	keys := []string{"earth", "mars", "jupiter", "saturn", "mercury", "venus", "neptune"}
	keysLength := len(keys)
	for i := 0; i < b.N; i++ {
		_, err := s.Get(keys[i%keysLength])
		assert.Nil(b, err)
	}
}

func BenchmarkInMemoryKeyValueStore_Put(b *testing.B) {
	s := newTestInMemoryKV()
	kvs := []*KeyValue{
		{"earth", earth},
		{"mars", mars},
		{"jupiter", jupiter},
		{"saturn", saturn},
		{"mercury", mercury},
		{"venus", venus},
		{"neptune", neptune},
	}
	kvsLength := len(kvs)
	for i := 0; i < b.N; i++ {
		kv := kvs[i%kvsLength]
		err := s.Put(kv.Key, kv.Value)
		assert.Nil(b, err)
	}
}
