package kasper

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func newTestStoreMetrics() (Store, *Map) {
	s := &Map{
		m: make(map[string][]byte, 10),
	}
	return s.WithMetrics(&noopMetricsProvider{}, "test"), s
}

func TestStoreMetrics_Get_ExistingKey(t *testing.T) {
	s, _ := newTestStoreMetrics()
	s.Put("mercury", mercury)
	actual, err := s.Get("mercury")
	assert.Equal(t, nil, err)
	assert.Equal(t, mercury, actual)
}

func TestStoreMetrics_Get_MissingKey(t *testing.T) {
	s, _ := newTestStoreMetrics()
	s.Put("earth", earth)
	actual, err := s.Get("venus")
	assert.Nil(t, err)
	assert.Nil(t, actual)
}

func TestStoreMetrics_Put_Normal(t *testing.T) {
	s, m := newTestStoreMetrics()
	err := s.Put("venus", venus)
	assert.Nil(t, err)
	assert.Equal(t, venus, m.m["venus"])
}

func TestStoreMetrics_Put_Overwrite(t *testing.T) {
	s, m := newTestStoreMetrics()
	s.Put("earth", jupiter)
	err := s.Put("earth", earth)
	assert.Nil(t, err)
	assert.Equal(t, earth, m.m["earth"])
}

func TestStoreMetrics_Delete_Existing(t *testing.T) {
	s, m := newTestStoreMetrics()
	m.m["neptune"] = neptune
	err := s.Delete("neptune")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(m.m))
}

func TestStoreMetrics_Delete_NonExisting(t *testing.T) {
	s, m := newTestStoreMetrics()
	m.m["uranus"] = uranus
	err := s.Delete("mercury")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(m.m))
}

func TestStoreMetrics_GetAll(t *testing.T) {
	s, m := newTestStoreMetrics()
	m.m["mars"] = mars
	kvs, err := s.GetAll([]string{"venus", "mars"})
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(kvs))
	assert.Equal(t, kvs["mars"], mars)
}

func TestStoreMetrics_GetAll_Empty(t *testing.T) {
	s, _ := newTestStoreMetrics()
	_, err := s.GetAll([]string{})
	assert.Nil(t, err)
	_, err = s.GetAll(nil)
	assert.Nil(t, err)
}

func TestStoreMetrics_PutAll(t *testing.T) {
	s, m := newTestStoreMetrics()
	err := s.PutAll(map[string][]byte{"jupiter": jupiter, "neptune": neptune})
	assert.Nil(t, err)
	assert.Equal(t, len(m.m), 2)
	assert.Equal(t, m.m["jupiter"], jupiter)
	assert.Equal(t, m.m["neptune"], neptune)
}

func TestStoreMetrics_PutAll_Empty(t *testing.T) {
	s, _ := newTestStoreMetrics()
	err := s.PutAll(map[string][]byte{})
	assert.Nil(t, err)
	err = s.PutAll(nil)
	assert.Nil(t, err)
}

func TestStoreMetrics_Flush(t *testing.T) {
	s, _ := newTestStoreMetrics()
	err := s.Flush()
	assert.Nil(t, err)
}
