package kasper

import (
	"log"
	"fmt"
	"time"
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
	item, err := store.Get("kasper/dragon/vorgansharax")
	assert.Nil(t, item)
	assert.Nil(t, err)

	// Put key
	err = store.Put("kasper/dragon/vorgansharax", &Dragon{"green", "Vorgansharax"})
	assert.Nil(t, err)

	// Get key again, should find it this time
	item, err = store.Get("kasper/dragon/vorgansharax")
	assert.NotNil(t, item)
	assert.Nil(t, err)
	dragon := item.(*Dragon)
	assert.Equal(t, &Dragon{"green", "Vorgansharax"}, dragon)

	// Test failed PUT (400)
	now := fmt.Sprintf("%d", time.Now().UnixNano())
	// First trick Elasticsearch into thinking Color is a date field...
	key := fmt.Sprintf("kasper-%s/dragon-%s/vorgansharax", now, now)
	err = store.Put(key, &Dragon{"2009-11-15T14:12:12", "Vorgansharax"})
	assert.Nil(t, err)
	// Then try to put a regular string in
	err = store.Put(key, &Dragon{"", "Vorgansharax"})
	assert.NotNil(t, err)
}

func TestElasticsearchKeyValueStore_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Put key
	err := store.Put("kasper/dragon/falkor", &Dragon{"white", "Falkor"})
	assert.Nil(t, err)

	// Delete key
	err = store.Delete("kasper/dragon/falkor")
	assert.Nil(t, err)

	// Get key again, should not find it this time
	item, err := store.Get("kasper/dragon/falkor")
	assert.Nil(t, err)
	assert.Nil(t, item)

	// Delete key again does nothing
	err = store.Delete("kasper/dragon/falkor")
	assert.Nil(t, err)
}

func TestElasticsearchKeyValueStore_GetAll_PutAll(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Put 3 keys
	err := store.Put("kasper/dragon/saphira", &Dragon{"blue", "Saphira"})
	assert.Nil(t, err)
	err = store.Put("kasper/dragon/mushu", &Dragon{"red", "Mushu"})
	assert.Nil(t, err)
	err = store.Put("kasper/dragon/fin-fang-foom", &Dragon{"green", "Fin Fang Foom"})
	assert.Nil(t, err)

	// GetAll on 4 keys, one non existing
	kvs, err := ToMap(store.GetAll([]string{
		"kasper/dragon/saphira",
		"kasper/dragon/draco",
		"kasper/dragon/mushu",
		"kasper/dragon/fin-fang-foom",
	}))
	assert.Nil(t, err)

	// Check the 3 keys
	assert.Equal(t, 3, len(kvs))
	assert.Equal(t, &Dragon{"blue", "Saphira"}, kvs["kasper/dragon/saphira"])
	assert.Equal(t, &Dragon{"red", "Mushu"}, kvs["kasper/dragon/mushu"])
	assert.Equal(t, &Dragon{"green", "Fin Fang Foom"}, kvs["kasper/dragon/fin-fang-foom"])

	// Delete everything
	_, err = store.client.DeleteIndex("kasper").Do(store.context)
	assert.Nil(t, err)

	// PutAll all 3 dragons again
	err = store.PutAll(FromMap(kvs))
	assert.Nil(t, err)

	// Check the 3 keys once more
	kvs, err = ToMap(store.GetAll([]string{
		"kasper/dragon/saphira",
		"kasper/dragon/mushu",
		"kasper/dragon/fin-fang-foom",
	}))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvs))
	assert.Equal(t, &Dragon{"blue", "Saphira"}, kvs["kasper/dragon/saphira"])
	assert.Equal(t, &Dragon{"red", "Mushu"}, kvs["kasper/dragon/mushu"])
	assert.Equal(t, &Dragon{"green", "Fin Fang Foom"}, kvs["kasper/dragon/fin-fang-foom"])

	// Test failed PUT (400)
	now := fmt.Sprintf("%d", time.Now().UnixNano())
	// First trick Elasticsearch into thinking Color is a date field...
	keyPrefix := fmt.Sprintf("kasper-%s/dragon-%s/", now, now)
	err = store.Put(fmt.Sprintf("%s%s", keyPrefix, "vorgansharax"), &Dragon{"2009-11-15T14:12:12", "Vorgansharax"})
	assert.Nil(t, err)
	// Then try to put a regular string in
	err = store.PutAll(FromMap(map[string]interface{}{
		fmt.Sprintf("%s%s", keyPrefix, "vorgansharax1"): &Dragon{"blue", "Vorgansharax1"},
		fmt.Sprintf("%s%s", keyPrefix, "vorgansharax2"): &Dragon{"blue", "Vorgansharax2"},
		fmt.Sprintf("%s%s", keyPrefix, "vorgansharax3"): &Dragon{"blue", "Vorgansharax3"},
		fmt.Sprintf("%s%s", keyPrefix, "vorgansharax4"): &Dragon{"blue", "Vorgansharax4"},
		fmt.Sprintf("%s%s", keyPrefix, "vorgansharax5"): &Dragon{"blue", "Vorgansharax5"},
		fmt.Sprintf("%s%s", keyPrefix, "vorgansharax6"): &Dragon{"blue", "Vorgansharax6"},
		fmt.Sprintf("%s%s", keyPrefix, "vorgansharax7"): &Dragon{"blue", "Vorgansharax7"},
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

func TestElasticsearchKeyValueStore_InvalidKey(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	assert.Panics(t, func() {
		store.Get("foo")
	})
	assert.Panics(t, func() {
		store.Put("foo", &Dragon{})
	})
	assert.Panics(t, func() {
		store.GetAll([]string{"foo"})
	})
	assert.Panics(t, func() {
		store.PutAll([]*KeyValue{{"foo", &Dragon{}}})
	})
	assert.Panics(t, func() {
		store.Delete("foo")
	})
}

func init() {
	SetLogger(&NoopLogger{})
	if !testing.Short() {
		store = NewElasticsearchKeyValueStore("http://localhost:9200", &Dragon{})
		store.client.DeleteIndex("kasper").Do(store.context)
	}
}

func ExampleNewElasticsearchKeyValueStore() {
	type User struct {
		name string
		age  int
	}

	// create new ES connection,
	store := NewElasticsearchKeyValueStore("http://localhost:9200", &User{})
	store.Put("users/user/john", User{
		name: "John Smith",
		age:  48,
	})

	// userItem is an abstract interface
	userItem, err := store.Get("users/user/john")
	if err != nil {
		// handle situation when ES store returns internal error, or value is not a valid JSON string
		panic(fmt.Sprintf("Something is wrong with your ES store: %s", err))
	}
	if userItem == nil {
		// handle not found key error
		panic("Key users/user/john not found")
	}

	// to use userItem as User instance, we should cast it
	user := userItem.(*User)
	user.age++
	log.Println(user)
}

func ExampleNewElasticsearchKeyValueStoreWithOpts() {
	type User struct {
		name string
		age  int
	}

	// use custom settings and mamppins for new indexes
	customElasticsearchOpts := ElasticsearchOpts{
		GetIndexSettings: func(indexName string) string {
			return `{
				"number_of_replicas": 3
			}`
		},
		GetIndexMappings: func(indexName string) string {
			return `{
				"_all" : {
					"enabled" : false
				},
			}`
		},
	}

	// use custom metrics provider
	prometheusMetricsProvider := NewPrometheusMetricsProvider("my-topic-processor", 0)

	// create new ES connection,
	store := NewElasticsearchKeyValueStoreWithOpts(
		"http://localhost:9200",
		&User{},
		&customElasticsearchOpts,
		prometheusMetricsProvider,
	)
	store.Put("users/user/john", User{
		name: "John Smith",
		age:  48,
	})

	// If index "users" did not exist, it is created with settings nad mappings you specified.
	// Also, store.Put method call already generated some prometheus metrics.
}
