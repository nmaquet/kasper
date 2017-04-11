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

func newTestMap() *Map {
	s := &Map{
		m: make(map[string][]byte, 10),
	}
	return s
}

func TestMap(t *testing.T) {
	NewMap(10)
}

func TestMap_Get_ExistingKey(t *testing.T) {
	s := newTestMap()
	s.Put("mercury", mercury)
	actual, err := s.Get("mercury")
	assert.Equal(t, nil, err)
	assert.Equal(t, mercury, actual)
}

func TestMap_Get_MissingKey(t *testing.T) {
	s := newTestMap()
	s.Put("earth", earth)
	actual, err := s.Get("venus")
	assert.Nil(t, err)
	assert.Nil(t, actual)
}

func TestMap_Put_Normal(t *testing.T) {
	s := newTestMap()
	err := s.Put("venus", venus)
	assert.Nil(t, err)
	assert.Equal(t, venus, s.m["venus"])
}

func TestMap_Put_Overwrite(t *testing.T) {
	s := newTestMap()
	s.Put("earth", jupiter)
	err := s.Put("earth", earth)
	assert.Nil(t, err)
	assert.Equal(t, earth, s.m["earth"])
}

func TestMap_Delete_Existing(t *testing.T) {
	s := newTestMap()
	s.m["neptune"] = neptune
	err := s.Delete("neptune")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(s.m))
}

func TestMap_Delete_NonExisting(t *testing.T) {
	s := newTestMap()
	s.m["uranus"] = uranus
	err := s.Delete("mercury")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(s.m))
}

func TestMap_GetAll(t *testing.T) {
	s := newTestMap()
	s.m["mars"] = mars
	kvs, err := s.GetAll([]string{"venus", "mars"})
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(kvs))
	assert.Equal(t, kvs["mars"], mars)
}

func TestMap_GetAll_Empty(t *testing.T) {
	s := newTestMap()
	_, err := s.GetAll([]string{})
	assert.Nil(t, err)
	_, err = s.GetAll(nil)
	assert.Nil(t, err)
}

func TestMap_PutAll(t *testing.T) {
	s := newTestMap()
	err := s.PutAll(map[string][]byte{"jupiter" : jupiter, "neptune": neptune})
	assert.Nil(t, err)
	assert.Equal(t, len(s.m), 2)
	assert.Equal(t, s.m["jupiter"], jupiter)
	assert.Equal(t, s.m["neptune"], neptune)
}

func TestMap_PutAll_Empty(t *testing.T) {
	s := newTestMap()
	err := s.PutAll(map[string][]byte{})
	assert.Nil(t, err)
	err = s.PutAll(nil)
	assert.Nil(t, err)
}

func TestMap_Flush(t *testing.T) {
	s := newTestMap()
	err := s.Flush()
	assert.Nil(t, err)
}

func BenchmarkMap_Get(b *testing.B) {
	s := newTestMap()
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

func BenchmarkMap_Put(b *testing.B) {
	s := newTestMap()
	kvs := map[string][]byte{
		"earth": earth,
		"mars": mars,
		"jupiter": jupiter,
		"saturn": saturn,
		"mercury": mercury,
		"venus": venus,
		"neptune": neptune,
	}
	keys := []string{
		"earth",
		"mars",
		"jupiter",
		"saturn",
		"mercury",
		"venus",
		"neptune",
	}
	for i := 0; i < b.N; i++ {
		key := keys[i % len(keys)]
		err := s.Put(key, kvs[key])
		assert.Nil(b, err)
	}
}
