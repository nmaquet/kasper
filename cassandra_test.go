package kasper

import (
	cassandra "github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
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
}

func init() {
	if testing.Short() {
		return
	}
	config := &Config{
		TopicProcessorName: "test",
		Logger:             &noopLogger{},
		MetricsProvider:    &noopMetricsProvider{},
	}
	cluster := cassandra.NewCluster(getCIHost())
	cluster.Consistency = cassandra.LocalOne
	cluster.Timeout = time.Second * 5
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
	cassandraStore = NewCassandra(config, session, CassandraMapping{
		keySpace:        "kasper",
		tableName:       "test",
		primaryKeyName:  "key",
		valueColumnName: "value",
	})
}
