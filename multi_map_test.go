package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultitenantInMemoryKVStore_ForTenant(t *testing.T) {
	mtkv := NewMultiMap(10)
	testMultiStore(t, mtkv)
}

func testMultiStore(t *testing.T, mtkv MultiStore) {
	spiderman := []byte(`{"name":"Spiderman","power":"webs"}`)
	ironman := []byte(`{"name":"Ironman","power":"a kickass powered armor"}`)
	batman := []byte(`{"name":"Batman","power":"money and an inflated sense of self"}`)
	superman := []byte(`{"name":"Superman","power":"not being recognized by wearing glasses"}`)

	err := mtkv.Tenant("marvel").Put("spiderman", spiderman)
	assert.Nil(t, err)
	err = mtkv.Tenant("marvel").Put("ironman", ironman)
	assert.Nil(t, err)
	err = mtkv.Tenant("dc").Put("batman", batman)
	assert.Nil(t, err)
	err = mtkv.Tenant("dc").Put("superman", superman)
	assert.Nil(t, err)

	heroes := []string{"spiderman", "ironman", "batman", "superman"}

	marvel, err := mtkv.Tenant("marvel").GetAll(heroes)
	assert.Nil(t, err)
	assert.NotNil(t, marvel["spiderman"])
	assert.NotNil(t, marvel["ironman"])
	assert.Nil(t, marvel["batman"])
	assert.Nil(t, marvel["superman"])

	dc, err := mtkv.Tenant("dc").GetAll(heroes)
	assert.Nil(t, err)
	assert.Nil(t, dc["spiderman"])
	assert.Nil(t, dc["ironman"])
	assert.NotNil(t, dc["batman"])
	assert.NotNil(t, dc["superman"])

	assert.Equal(t, []string{"dc", "marvel"}, mtkv.AllTenants())

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
}
