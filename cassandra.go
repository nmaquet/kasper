package kasper

import (
	cassandra "github.com/gocql/gocql"
)

type CassandraQueries interface {
	Select(keys []string) (string, []interface{})
	Insert(key string, value []byte) (string, []interface{})
	Delete(key string) (string, []interface{})
}

// Cassandra is an implementation of Store that uses Cassandra.
// FIXME: add doc
type Cassandra struct {
	session       *cassandra.Session
	queries       CassandraQueries
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
func NewCassandra(config *Config, session *cassandra.Session, mapping CassandraQueries) *Cassandra {
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
	statement, bindings := c.queries.Select([]string{key})
	var value []byte
	iter := c.session.Query(statement, bindings...).Iter()
	if iter.NumRows() == 0 {
		return nil, iter.Close()
	}
	iter.Scan(&key, &value)
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return value, nil
}

func (c *Cassandra) GetAll(keys []string) (map[string][]byte, error) {
	statement, bindings := c.queries.Select(keys)
	iter := c.session.Query(statement, bindings...).Iter()
	result := make(map[string][]byte, len(keys))
	var key string
	var value []byte
	for iter.Scan(&key, &value) {
		result[key] = value
	}
	err := iter.Close()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Cassandra) Put(key string, value []byte) error {
	statement, bindings := c.queries.Insert(key, value)
	query := c.session.Query(statement, bindings...)
	return query.Exec()
}

func (c *Cassandra) PutAll(entries map[string][]byte) error {
	batch := c.session.NewBatch(cassandra.LoggedBatch)
	for key, value := range entries {
		statement, bindings := c.queries.Insert(key, value)
		batch.Query(statement, bindings...)
	}
	return c.session.ExecuteBatch(batch)
}

func (c *Cassandra) Delete(key string) error {
	statement, bindings := c.queries.Delete(key)
	query := c.session.Query(statement, bindings...)
	return query.Exec()
}

func (c *Cassandra) Flush() error {
	panic("Cassandra.Flush() is not supported.")
}
