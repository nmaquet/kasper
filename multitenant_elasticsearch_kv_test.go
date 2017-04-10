package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultitenantElasticsearchKVStore(t *testing.T) {
	mtkv := NewMultitenantElasticsearchKVStore(
		"http://localhost:9200",
		"hero",
	)
	testMultiTenantKeyValueStore(t, mtkv)
}

func TestMultitenantElasticsearchKVStore_PutAll_GetAll(t *testing.T) {

	mtkv := NewMultitenantElasticsearchKVStore(
		"http://localhost:9200",
		"hero",
	)

	spiderman := []byte(`{"name":"Spiderman","power":"webs"}`)
	ironman := []byte(`{"name":"Ironman","power":"a kickass powered armor"}`)
	batman := []byte(`{"name":"Batman","power":"money and an inflated sense of self"}`)
	superman := []byte(`{"name":"Superman","power":"not being recognized by wearing glasses"}`)

	err := mtkv.Tenant("marvel").Put("spiderman", spiderman)
	assert.Nil(t, err)
	err = mtkv.Tenant("dc").Put("batman", batman)
	assert.Nil(t, err)

	s, err := mtkv.Fetch([]TenantKey{{"marvel", "spiderman"}, {"dc", "batman"}})
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
