package kasper

import (
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

	mtkv.Tenant("marvel").Put("spiderman", spiderman)
	mtkv.Tenant("dc").Put("batman", batman)

	s, err := mtkv.GetAll([]*TenantKey{{"marvel", "spiderman"}, {"dc", "batman"}})
	assert.Nil(t, err)

	hero, _ := s.Tenant("marvel").Get("spiderman")
	assert.Equal(t, spiderman, hero)

	hero, _ = s.Tenant("dc").Get("batman")
	assert.Equal(t, batman, hero)

	s.Tenant("marvel").Put("ironman", ironman)
	s.Tenant("dc").Put("superman", superman)

	err = mtkv.PutAll(s)
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
