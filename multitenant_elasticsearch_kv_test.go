package kasper

import (
	"testing"
)

func TestMultitenantElasticsearchKVStore(t *testing.T) {
	mtkv := NewMultitenantElasticsearchKVStore(
		"http://localhost:9200",
		"hero",
		&Hero{},
	)
	testMultiTenantKeyValueStore(t, mtkv)
}

func init() {
	SetLogger(&NoopLogger{})
}
