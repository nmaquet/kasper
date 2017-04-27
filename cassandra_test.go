package kasper

import (
	"fmt"
	cassandra "github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

var cassandraStore *Cassandra

func TestCassandra_Get_Put(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Get non-existing key
	item, err := cassandraStore.Get("vorgansharax")
	assert.Nil(t, item)
	assert.Nil(t, err)

	// Put key
	err = cassandraStore.Put("vorgansharax", vorgansharax)
	assert.Nil(t, err)

	// Get key again, should find it this time
	dragon, err := cassandraStore.Get("vorgansharax")
	assert.Equal(t, vorgansharax, dragon)

	// Delete key
	err = cassandraStore.Delete("vorgansharax")
	assert.Nil(t, err)

	// Get key again, should not find it anymore
	item, err = cassandraStore.Get("vorgansharax")
	assert.Nil(t, item)
	assert.Nil(t, err)
}

func TestCassandra_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Delete non-existing key
	err := cassandraStore.Delete("saphira")
	assert.Nil(t, err)

	// Put key
	err = cassandraStore.Put("saphira", saphira)
	assert.Nil(t, err)

	// Delete key again, should work this time
	err = cassandraStore.Delete("saphira")
	assert.Nil(t, err)

	// Get key again, should not find it anymore
	dragon, err := cassandraStore.Get("saphira")
	assert.Nil(t, dragon)
	assert.Nil(t, err)
}

func TestCassandra_GetAll(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	err := cassandraStore.Put("vorgansharax", vorgansharax)
	assert.Nil(t, err)

	err = cassandraStore.Put("saphira", saphira)
	assert.Nil(t, err)

	err = cassandraStore.Put("falkor", falkor)
	assert.Nil(t, err)

	kvs, err := cassandraStore.GetAll([]string{"vorgansharax", "saphira", "batman"})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(kvs))
	assert.Equal(t, vorgansharax, kvs["vorgansharax"])
	assert.Equal(t, saphira, kvs["saphira"])
}

func TestCassandra_PutAll(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	err := cassandraStore.PutAll(map[string][]byte{
		"vorgansharax": vorgansharax,
		"saphira":      saphira,
		"falkor":       falkor,
	})
	assert.Nil(t, err)

	kvs, err := cassandraStore.GetAll([]string{"vorgansharax", "saphira", "batman"})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(kvs))
	assert.Equal(t, vorgansharax, kvs["vorgansharax"])
	assert.Equal(t, saphira, kvs["saphira"])
}

type testMapping struct{}

func (testMapping) Select(keys []string) (string, []interface{}) {
	bindings := make([]interface{}, len(keys))
	for i := range keys {
		bindings[i] = keys[i]
	}
	format := "SELECT key, value FROM kasper.test WHERE key in (%s)"
	statement := fmt.Sprintf(format, strings.Repeat(",?", len(keys))[1:])
	return statement, bindings
}

func (testMapping) Insert(key string, value []byte) (string, []interface{}) {
	return "INSERT INTO kasper.test (key, value) VALUES (?, ?)", []interface{}{key, value}
}

func (testMapping) Delete(key string) (string, []interface{}) {
	return "DELETE FROM kasper.test WHERE key = ?", []interface{}{key}
}

func init() {
	if testing.Short() {
		return
	}
	config := &Config{
		TopicProcessorName: "test",
		Logger:             NewBasicLogger(true),
		MetricsProvider:    &noopMetricsProvider{},
	}
	cluster := cassandra.NewCluster(getCIHost())
	cluster.Consistency = cassandra.LocalOne
	cluster.Timeout = time.Second * 10
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	err = session.Query("DROP KEYSPACE IF EXISTS kasper").Exec()
	if err != nil {
		panic(err)
	}
	err = session.Query("CREATE KEYSPACE kasper WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };").Exec()
	if err != nil {
		panic(err)
	}
	err = session.Query("CREATE TABLE kasper.test(\"key\" text, \"value\" blob, PRIMARY KEY (\"key\"))").Exec()
	if err != nil {
		panic(err)
	}
	cassandraStore = NewCassandra(config, session, testMapping{})
}
