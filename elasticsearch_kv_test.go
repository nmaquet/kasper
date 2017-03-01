package kasper

import (
	"log"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Dragon struct {
	Color string `json:"color"`
	Name  string `json:"name"`
}

var store *ElasticsearchKeyValueStore

func TestElasticsearchKeyValue_Get_Put(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Get non-existing key
	item, err := store.Get("vorgansharax")
	assert.Nil(t, item)
	assert.Nil(t, err)

	// Put key
	err = store.Put("vorgansharax", &Dragon{"green", "Vorgansharax"})
	assert.Nil(t, err)

	// Get key again, should find it this time
	item, err = store.Get("vorgansharax")
	assert.NotNil(t, item)
	assert.Nil(t, err)
	dragon := item.(*Dragon)
	assert.Equal(t, &Dragon{"green", "Vorgansharax"}, dragon)

	// Test failed PUT (400)
	_, err = store.client.DeleteIndex("kasper").Do(store.context)
	defer store.client.DeleteIndex("kasper").Do(store.context)
	assert.Nil(t, err)
	// First trick Elasticsearch into thinking Color is a date field...
	err = store.Put("vorgansharax", &Dragon{"2009-11-15T14:12:12", "Vorgansharax"})
	assert.Nil(t, err)
	// Then try to put a regular string in
	err = store.Put("vorgansharax", &Dragon{"", "Vorgansharax"})
	assert.NotNil(t, err)

}

func TestElasticsearchKeyValueStore_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Put key
	err := store.Put("falkor", &Dragon{"white", "Falkor"})
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

func TestElasticsearchKeyValueStore_GetAll_PutAll(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Put 3 keys
	err := store.Put("saphira", &Dragon{"blue", "Saphira"})
	assert.Nil(t, err)
	err = store.Put("mushu", &Dragon{"red", "Mushu"})
	assert.Nil(t, err)
	err = store.Put("fin-fang-foom", &Dragon{"green", "Fin Fang Foom"})
	assert.Nil(t, err)

	// GetAll on 4 keys, one non existing
	kvs, err := ToMap(store.GetAll([]string{
		"saphira",
		"draco",
		"mushu",
		"fin-fang-foom",
	}))
	assert.Nil(t, err)

	// Check the 3 keys
	assert.Equal(t, 3, len(kvs))
	assert.Equal(t, &Dragon{"blue", "Saphira"}, kvs["saphira"])
	assert.Equal(t, &Dragon{"red", "Mushu"}, kvs["mushu"])
	assert.Equal(t, &Dragon{"green", "Fin Fang Foom"}, kvs["fin-fang-foom"])

	// Delete everything
	_, err = store.client.DeleteIndex("kasper").Do(store.context)
	assert.Nil(t, err)

	// PutAll all 3 dragons again
	err = store.PutAll(FromMap(kvs))
	assert.Nil(t, err)

	// Check the 3 keys once more
	kvs, err = ToMap(store.GetAll([]string{
		"saphira",
		"mushu",
		"fin-fang-foom",
	}))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvs))
	assert.Equal(t, &Dragon{"blue", "Saphira"}, kvs["saphira"])
	assert.Equal(t, &Dragon{"red", "Mushu"}, kvs["mushu"])
	assert.Equal(t, &Dragon{"green", "Fin Fang Foom"}, kvs["fin-fang-foom"])

	// Test failed PUT (400)
	_, err = store.client.DeleteIndex("kasper").Do(store.context)
	assert.Nil(t, err)
	// First trick Elasticsearch into thinking Color is a date field...
	err = store.Put("vorgansharax", &Dragon{"2009-11-15T14:12:12", "Vorgansharax"})
	assert.Nil(t, err)
	// Then try to put a regular string in
	err = store.PutAll(FromMap(map[string]interface{}{
		"vorgansharax1": &Dragon{"blue", "Vorgansharax1"},
		"vorgansharax2": &Dragon{"blue", "Vorgansharax2"},
		"vorgansharax3": &Dragon{"blue", "Vorgansharax3"},
		"vorgansharax4": &Dragon{"blue", "Vorgansharax4"},
		"vorgansharax5": &Dragon{"blue", "Vorgansharax5"},
		"vorgansharax6": &Dragon{"blue", "Vorgansharax6"},
		"vorgansharax7": &Dragon{"blue", "Vorgansharax7"},
	}))
	assert.NotNil(t, err)

	// GetAll of empty list should work
	_, err = store.GetAll(nil)
	assert.Nil(t, err)
	_, err = store.GetAll([]string{})
	assert.Nil(t, err)
}

func TestElasticsearchKeyValueStore_Flush(t *testing.T) {
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
	store = NewElasticsearchKeyValueStore(
		"http://localhost:9200",
		"kasper",
		"dragon",
		&Dragon{},
	)
	store.client.DeleteIndex("kasper").Do(store.context)
	store.checkOrCreateIndex()
}

func ExampleNewElasticsearchKeyValueStore() {
	type User struct {
		name string
		age  int
	}
	store := NewElasticsearchKeyValueStore(
		"http://localhost:9200",
		"users",
		"user",
		&User{},
	)
	store.Put("john", User{
		name: "John Smith",
		age:  48,
	})
	userItem, err := store.Get("john")
	if err != nil {
		panic(fmt.Sprintf("Something is wrong with your ES store: %s", err))
	}
	if userItem == nil {
		panic("Key john not found")
	}
	user := userItem.(*User)
	user.age++
	log.Println(user)
}

func ExampleNewElasticsearchKeyValueStoreWithOptions() {
	type User struct {
		Name string `json:"name"`
		Age  int `json:"age"`
	}
	prometheusMetricsProvider := NewPrometheusMetricsProvider(
		"my-topic-processor",
		0,
	)
	store := NewElasticsearchKeyValueStoreWithMetrics(
		"http://localhost:9200",
		"users",
		"user",
		&User{},
		prometheusMetricsProvider,
	)
	store.IndexSettings = `{
		"number_of_replicas": 3
	}`
	store.TypeMapping = `{
		"user" : {
			"properties" : {
				"name": {
					"type": "string"
				},
				"age": {
					"type": "long"
				}
			}
		}
	}`
	store.Put("john", User{
		Name: "John Smith",
		Age:  48,
	})
}
