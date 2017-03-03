package kasper

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultitenantElasticsearchKVStore(t *testing.T) {
	mtkv := NewMultitenantElasticsearchKVStore(
		"http://localhost:9200",
		"hero",
		&Hero{},
	)
	testMultiTenantKeyValueStore(t, mtkv)
}

func TestMultitenantElasticsearchKVStore_PutAll_GetAll(t *testing.T) {

	mtkv := NewMultitenantElasticsearchKVStore(
		"http://localhost:9200",
		"hero",
		&Hero{},
	)

	spiderman := &Hero{"Spiderman", "webs"}
	ironman := &Hero{"Ironman", "a kickass powered armor"}
	batman := &Hero{"Batman", "money and an inflated sense of self"}
	superman := &Hero{"Superman", "not being recognized by wearing glasses"}

	err := mtkv.Tenant("marvel").Put("spiderman", spiderman)
	assert.Nil(t, err)
	err = mtkv.Tenant("dc").Put("batman", batman)
	assert.Nil(t, err)

	s, err := mtkv.Fetch([]*TenantKey{{"marvel", "spiderman"}, {"dc", "batman"}})
	assert.Nil(t, err)

	hero, _ := s.Tenant("marvel").Get("spiderman")
	assert.Equal(t, spiderman, hero)

	hero, _ = s.Tenant("dc").Get("batman")
	assert.Equal(t, batman, hero)

	err = s.Tenant("marvel").Put("ironman", ironman)
	assert.Nil(t, err)
	err = s.Tenant("dc").Put("superman", superman)
	assert.Nil(t, err)

	err = mtkv.Push(s)
	assert.Nil(t, err)

	hero, err = mtkv.Tenant("marvel").Get("ironman")
	assert.Nil(t, err)
	assert.Equal(t, ironman, hero)

	hero, err = mtkv.Tenant("dc").Get("superman")
	assert.Nil(t, err)
	assert.Equal(t, superman, hero)
}

func init() {
	SetLogger(&NoopLogger{})
}

func ExampleNewMultitenantElasticsearchKVStore() {
	type User struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	//connect to remote elasticsearch store
	store := NewMultitenantElasticsearchKVStore(
		"http://my.server.com:9200",
		"user",
		&User{},
	)

	localStore, err := store.Fetch([]*TenantKey{
		{
			Tenant: "england",
			Key:    "bumberstump",
		},
		{
			Tenant: "france",
			Key:    "coco",
		},
	})
	if err != nil {
		// something is wrong with remote store, deal with it
		panic(err)
	}

	// see what is inside entries we have just got
	item, _ := localStore.Tenant("france").Get("coco")
	cocoChanel := item.(*User)
	log.Print(cocoChanel.Age)

	// get tenant local store
	// it returns InMemoryKeyValueStore as KeyValueStore
	englishUserStore := localStore.Tenant("england")

	// add new user to local store
	englishUserStore.Put("bombadil", &User{
		Name: "Bombadil Clombyclomp",
		Age:  19,
	})

	// change existing user in local store
	// to do than, we should first fetch item from remote store
	// by passing TenantKey for it
	item, _ = englishUserStore.Get("bumberstump")
	englishUserStore.Put("bumberstump", &User{
		Name: "Bumberstump Cheddarcheese",
		Age:  item.(*User).Age,
	})

	// push all entries from local store to remote ES store
	store.Push(localStore)

	if err != nil {
		panic(err)
	}
}
