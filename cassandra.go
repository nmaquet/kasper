package kasper

import (
	cassandra "github.com/gocql/gocql"
	"sync"
)

const CassandraBatchSize = 100

type CassandraQuery struct {
	Statement string
	Bindings  []interface{}
}

type CassandraQueries interface {
	Select(keys []string) CassandraQuery
	Insert(key string, value []byte) CassandraQuery
	Delete(key string) CassandraQuery
}

// Cassandra is an implementation of Store that uses Cassandra.
// FIXME: add doc
type Cassandra struct {
	session *cassandra.Session
	queries CassandraQueries
	logger  Logger

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
func NewCassandra(config *Config, session *cassandra.Session, queries CassandraQueries) *Cassandra {
	metrics := config.MetricsProvider
	labelNames := []string{"topicProcessor"}
	return &Cassandra{
		session,
		queries,
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
	query := c.queries.Select([]string{key})
	var value []byte
	iter := c.session.Query(query.Statement, query.Bindings...).Iter()
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
	query := c.queries.Select(keys)
	iter := c.session.Query(query.Statement, query.Bindings...).Iter()
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
	query := c.queries.Insert(key, value)
	return c.session.Query(query.Statement, query.Bindings...).Exec()
}

func (c *Cassandra) PutAll(entries map[string][]byte) error {
	batch := make(map[string][]byte, CassandraBatchSize)
	var err error
	var wg sync.WaitGroup
	i := 0
	for key, value := range entries {
		i += 1
		batch[key] = value
		if i%CassandraBatchSize == 0 || i == len(entries) {
			wg.Add(1)
			go c.putAllSync(batch, &wg, &err)
			batch = make(map[string][]byte, CassandraBatchSize)
		}
	}
	wg.Wait()
	return err
}

func (c *Cassandra) Delete(key string) error {
	query := c.queries.Delete(key)
	return c.session.Query(query.Statement, query.Bindings...).Exec()
}

func (c *Cassandra) Flush() error {
	panic("Cassandra.Flush() is not supported.")
}

func (c *Cassandra) putAllSync(entries map[string][]byte, wg *sync.WaitGroup, err *error) {
	batch := c.session.NewBatch(cassandra.LoggedBatch)
	for key, value := range entries {
		query := c.queries.Insert(key, value)
		batch.Query(query.Statement, query.Bindings...)
	}
	batchErr := c.session.ExecuteBatch(batch)
	if batchErr != nil {
		*err = batchErr
	}
	wg.Done()
}
