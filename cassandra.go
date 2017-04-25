package kasper

import (
	"fmt"
	cassandra "github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type CassandraMapping struct {
	keySpace        string
	tableName       string
	primaryKeyName  string
	valueColumnName string
}

// Cassandra is an implementation of Store that uses Cassandra.
// FIXME: add doc
type Cassandra struct {
	session       *cassandra.Session
	mapping       CassandraMapping
	logger        Logger
	labelValues   []string
	getCounter    Counter
	getAllSummary Summary
	putCounter    Counter
	putAllSummary Summary
	deleteCounter Counter
	flushCounter  Counter
}

// NewCassandra creates Cassandra instances. All keys read and written in Cassandra are of the form:
// FIXME: add doc
func NewCassandra(config *Config, session *cassandra.Session, mapping CassandraMapping) *Cassandra {
	metrics := config.MetricsProvider
	labelNames := []string{"topicProcessor"}
	return &Cassandra{
		session,
		mapping,
		config.Logger,
		[]string{config.TopicProcessorName},
		metrics.NewCounter("Cassandra_Get", "Number of Get() calls", labelNames...),
		metrics.NewSummary("Cassandra_GetAll", "Summary of GetAll() calls", labelNames...),
		metrics.NewCounter("Cassandra_Put", "Number of Put() calls", labelNames...),
		metrics.NewSummary("Cassandra_PutAll", "Summary of PutAll() calls", labelNames...),
		metrics.NewCounter("Cassandra_Delete", "Number of Delete() calls", labelNames...),
		metrics.NewCounter("Cassandra_Flush", "Summary of Flush() calls", labelNames...),
	}
}

func (c *Cassandra) Get(key string) ([]byte, error) {
	fmtStr := "SELECT \"%s\" FROM \"%s\".\"%s\" WHERE \"%s\" = ?"
	queryStr := fmt.Sprintf(
		fmtStr,
		c.mapping.valueColumnName,
		c.mapping.keySpace,
		c.mapping.tableName,
		c.mapping.primaryKeyName,
	)
	iter := c.session.Query(queryStr, key).Iter()
	if iter.NumRows() == 0 {
		return nil, nil
	}
	var data []byte
	if !iter.Scan(&data) {
		return nil, errors.New("Failed to scan Cassandra value")
	}
	return data, nil
}

func (c *Cassandra) GetAll(keys []string) (map[string][]byte, error) {
	panic("not implemented")
}

func (c *Cassandra) Put(key string, value []byte) error {
	fmtStr := "INSERT INTO \"%s\".\"%s\" (\"%s\", \"%s\") VALUES (?, ?)"
	queryStr := fmt.Sprintf(fmtStr,
		c.mapping.keySpace,
		c.mapping.tableName,
		c.mapping.primaryKeyName,
		c.mapping.valueColumnName,
	)
	query := c.session.Query(queryStr, key, value)
	return query.Exec()
}

func (c *Cassandra) PutAll(entries map[string][]byte) error {
	panic("not implemented")
}

func (c *Cassandra) Delete(key string) error {
	panic("not implemented")
}

func (c *Cassandra) Flush() error {
	panic("not implemented")
}
