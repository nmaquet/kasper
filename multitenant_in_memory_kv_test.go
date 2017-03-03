package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Hero struct {
	Name  string `json:"name"`
	Power string `json:"power"`
}

func TestMultitenantInMemoryKVStore_ForTenant(t *testing.T) {
	mtkv := NewMultitenantInMemoryKVStore(10, &Hero{})
	testMultiTenantKeyValueStore(t, mtkv)
}

func testMultiTenantKeyValueStore(t *testing.T, mtkv MultitenantKeyValueStore) {
	spiderman := &Hero{"Spiderman", "webs"}
	ironman := &Hero{"Ironman", "a kickass powered armor"}
	batman := &Hero{"Batman", "money and an inflated sense of self"}
	superman := &Hero{"Superman", "not being recognized by wearing glasses"}

	err := mtkv.Tenant("marvel").Put("spiderman", spiderman)
	assert.Nil(t, err)
	err = mtkv.Tenant("marvel").Put("ironman", ironman)
	assert.Nil(t, err)
	err = mtkv.Tenant("dc").Put("batman", batman)
	assert.Nil(t, err)
	err = mtkv.Tenant("dc").Put("superman", superman)
	assert.Nil(t, err)

	heroes := []string{"spiderman", "ironman", "batman", "superman"}

	marvel, err := ToMap(mtkv.Tenant("marvel").GetAll(heroes))
	assert.Nil(t, err)
	assert.NotNil(t, marvel["spiderman"])
	assert.NotNil(t, marvel["ironman"])
	assert.Nil(t, marvel["batman"])
	assert.Nil(t, marvel["superman"])

	dc, err := ToMap(mtkv.Tenant("dc").GetAll(heroes))
	assert.Nil(t, err)
	assert.Nil(t, dc["spiderman"])
	assert.Nil(t, dc["ironman"])
	assert.NotNil(t, dc["batman"])
	assert.NotNil(t, dc["superman"])

	assert.Equal(t, []string{"dc", "marvel"}, mtkv.AllTenants())
}
