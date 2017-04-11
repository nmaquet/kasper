package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var store *Elasticsearch

var vorgansharax = []byte(`{"color": "green", "name": "Vorgansharax"}`)
var falkor = []byte(`{"color": "white", "name": "Falkor"}`)
var saphira = []byte(`{"color": "blue", "name": "Saphira"}`)
var mushu = []byte(`{"color": "red", "name": "Mushu"}`)
var finFangFoom = []byte(`{"color": "green", "name": "Fin Fang Foom"}`)

func TestElasticsearch_Get_Put(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Get non-existing key
	item, err := store.Get("vorgansharax")
	assert.Nil(t, item)
	assert.Nil(t, err)

	// Put key
	err = store.Put("vorgansharax", vorgansharax)
	assert.Nil(t, err)

	// Get key again, should find it this time
	dragon, err := store.Get("vorgansharax")
	assert.Equal(t, vorgansharax, dragon)

	// Test failed PUT (400)
	_, err = store.client.DeleteIndex("kasper").Do(store.context)
	assert.Nil(t, err)
	defer func() {
		_, err = store.client.DeleteIndex("kasper").Do(store.context)
		assert.Nil(t, err)
	}()
	// First trick MultiElasticsearch into thinking Color is a date field...
	err = store.Put("vorgansharax", []byte(`{"color": "2009-11-15T14:12:12", "name": "Vorgansharax"}`))
	assert.Nil(t, err)
	// Then try to put a regular string in
	err = store.Put("vorgansharax", []byte(`{"color": "", "name": "Vorgansharax"}`))
	assert.NotNil(t, err)

}

func TestElasticsearch_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Put key
	err := store.Put("falkor", falkor)
	assert.Nil(t, err)

	// Delete key
	err = store.Delete("falkor")
	assert.Nil(t, err)

	// Get key again, should not find it this time
	item, err := store.Get("falkor")
	assert.Nil(t, err)
	assert.Nil(t, item)

	// Delete key again does nothing
	err = store.Delete("falkor")
	assert.Nil(t, err)
}

func TestElasticsearch_GetAll_PutAll(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Put 3 keys
	err := store.Put("saphira", saphira)
	assert.Nil(t, err)
	err = store.Put("mushu", mushu)
	assert.Nil(t, err)
	err = store.Put("fin-fang-foom", finFangFoom)
	assert.Nil(t, err)

	// GetAll on 4 keys, one non existing
	kvs, err := store.GetAll([]string{
		"saphira",
		"draco",
		"mushu",
		"fin-fang-foom",
	})
	assert.Nil(t, err)

	// Check the 3 keys
	assert.Equal(t, 3, len(kvs))
	assert.Equal(t, saphira, kvs["saphira"])
	assert.Equal(t, mushu, kvs["mushu"])
	assert.Equal(t, finFangFoom, kvs["fin-fang-foom"])

	// Delete everything
	_, err = store.client.DeleteIndex("kasper").Do(store.context)
	assert.Nil(t, err)

	// PutAll all 3 dragons again
	err = store.PutAll(kvs)
	assert.Nil(t, err)

	// Check the 3 keys once more
	kvs, err = store.GetAll([]string{
		"saphira",
		"mushu",
		"fin-fang-foom",
	})
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvs))
	assert.Equal(t, saphira, kvs["saphira"])
	assert.Equal(t, mushu, kvs["mushu"])
	assert.Equal(t, finFangFoom, kvs["fin-fang-foom"])

	// Test failed PUT (400)
	_, err = store.client.DeleteIndex("kasper").Do(store.context)
	assert.Nil(t, err)
	// First trick MultiElasticsearch into thinking Color is a date field...
	err = store.Put("vorgansharax", []byte(`{"color": "2009-11-15T14:12:12", "name": "Vorgansharax"}`))
	assert.Nil(t, err)
	// Then try to put a regular string in
	err = store.PutAll(map[string][]byte{
		"vorgansharax1": vorgansharax,
		"vorgansharax2": vorgansharax,
		"vorgansharax3": vorgansharax,
		"vorgansharax4": vorgansharax,
		"vorgansharax5": vorgansharax,
		"vorgansharax6": vorgansharax,
		"vorgansharax7": vorgansharax,
	})
	assert.NotNil(t, err)

	// GetAll of empty list should work
	_, err = store.GetAll(nil)
	assert.Nil(t, err)
	_, err = store.GetAll([]string{})
	assert.Nil(t, err)
}

func TestElasticsearch_Flush(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	err := store.Flush()
	assert.Nil(t, err)
}

func init() {
	if testing.Short() {
		return
	}
	SetLogger(&NoopLogger{})
	store = NewElasticsearch(
		"http://localhost:9200",
		"kasper",
		"dragon",
	)
	_, err := store.client.DeleteIndex("kasper").Do(store.context)
	if err != nil {
		panic(err)
	}
	store.checkOrCreateIndex()
}
