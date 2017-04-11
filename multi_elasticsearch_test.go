package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestElasticsearchType(t *testing.T) {
	store := NewMultiElasticsearch(
		"http://localhost:9200",
		"hero",
	)
	testMultiStore(t, store)
}

func TestElasticsearchType_PutAll_GetAll(t *testing.T) {

	store := NewMultiElasticsearch(
		"http://localhost:9200",
		"hero",
	)

	spiderman := []byte(`{"name":"Spiderman","power":"webs"}`)
	ironman := []byte(`{"name":"Ironman","power":"a kickass powered armor"}`)
	batman := []byte(`{"name":"Batman","power":"money and an inflated sense of self"}`)
	superman := []byte(`{"name":"Superman","power":"not being recognized by wearing glasses"}`)

	err := store.Tenant("marvel").Put("spiderman", spiderman)
	assert.Nil(t, err)
	err = store.Tenant("dc").Put("batman", batman)
	assert.Nil(t, err)

	s, err := store.Fetch([]TenantKey{{"marvel", "spiderman"}, {"dc", "batman"}})
	assert.Nil(t, err)

	hero, _ := s.Tenant("marvel").Get("spiderman")
	assert.Equal(t, spiderman, hero)

	hero, _ = s.Tenant("dc").Get("batman")
	assert.Equal(t, batman, hero)

	err = s.Tenant("marvel").Put("ironman", ironman)
	assert.Nil(t, err)
	err = s.Tenant("dc").Put("superman", superman)
	assert.Nil(t, err)

	err = store.Push(s)
	assert.Nil(t, err)

	hero, err = store.Tenant("marvel").Get("ironman")
	assert.Nil(t, err)
	assert.Equal(t, ironman, hero)

	hero, err = store.Tenant("dc").Get("superman")
	assert.Nil(t, err)
	assert.Equal(t, superman, hero)
}

func init() {
	SetLogger(&NoopLogger{})
}
