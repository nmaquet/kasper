package kasper

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// Redis is an implementation of Store that uses Redis.
// Each instance provides key-value access to keys with a specific prefix.
// This implementation uses Gary Burd's Go Redis client.
// See https://github.com/garyburd/redigo
type Redis struct {
	conn      redis.Conn
	keyPrefix string

	logger        Logger
	labelValues   []string
	getCounter    Counter
	getAllSummary Summary
	putCounter    Counter
	putAllSummary Summary
	deleteCounter Counter
	flushCounter  Counter
}

// NewRedis creates Redis instances. All keys read and written in Redis are of the form:
//	{keyPrefix}/{key}
func NewRedis(config *Config, conn redis.Conn, keyPrefix string) *Redis {
	metrics := config.MetricsProvider
	labelNames := []string{"topicProcessor", "keyPrefix"}
	return &Redis{
		conn,
		keyPrefix,
		config.Logger,
		[]string{config.TopicProcessorName, keyPrefix},
		metrics.NewCounter("Redis_Get", "Number of Get() calls", labelNames...),
		metrics.NewSummary("Redis_GetAll", "Summary of GetAll() calls", labelNames...),
		metrics.NewCounter("Redis_Put", "Number of Put() calls", labelNames...),
		metrics.NewSummary("Redis_PutAll", "Summary of PutAll() calls", labelNames...),
		metrics.NewCounter("Redis_Delete", "Number of Delete() calls", labelNames...),
		metrics.NewCounter("Redis_Flush", "Summary of Flush() calls", labelNames...),
	}
}

func (s *Redis) getPrefixedKey(key string) string {
	return fmt.Sprintf("%s/%s", s.keyPrefix, key)
}

// Get gets a value by key.
// Returns nil, nil if the key is missing.
// It is implemented using the Redis GET command.
// See https://redis.io/commands/get
func (s *Redis) Get(key string) ([]byte, error) {
	s.logger.Debug("Redis Get: ", key)
	s.getCounter.Inc(s.labelValues...)
	value, err := s.conn.Do("GET", s.getPrefixedKey(key))
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	bytes, err := redis.Bytes(value, err)
	if err != nil {
		return nil, err
	}
	return bytes, err
}

// GetAll gets multiple values by key.
// It is implemented by using the MULTI and GET commands.
// See https://redis.io/commands/multi
func (s *Redis) GetAll(keys []string) (map[string][]byte, error) {
	s.getAllSummary.Observe(float64(len(keys)), s.labelValues...)
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}
	s.logger.Debug("Redis GetAll: ", keys)
	err := s.conn.Send("MULTI")
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		err = s.conn.Send("GET", s.getPrefixedKey(key))
		if err != nil {
			return nil, err
		}
	}
	values, err := redis.Values(s.conn.Do("EXEC"))
	if err != nil {
		return nil, err
	}
	entries := make(map[string][]byte, len(keys))
	for i, value := range values {
		if value == nil {
			continue
		}
		bytes, err := redis.Bytes(value, err)
		if err != nil {
			return nil, err
		}
		entries[keys[i]] = bytes
	}
	return entries, nil
}

// Puts inserts or updates a value by key.
// It is implemented using the Redis SET command.
// See https://redis.io/commands/set
func (s *Redis) Put(key string, value []byte) error {
	s.logger.Debugf("Redis Put: %s %#v", s.getPrefixedKey(key), value)
	s.putCounter.Inc(s.labelValues...)
	_, err := s.conn.Do("SET", s.getPrefixedKey(key), value)
	return err
}

// PutAll inserts or updates multiple values by key.
// It is implemented by using the MULTI and SET commands.
// See https://redis.io/commands/multi
func (s *Redis) PutAll(entries map[string][]byte) error {
	s.logger.Debugf("Redis PutAll of %d keys", len(entries))
	s.putAllSummary.Observe(float64(len(entries)), s.labelValues...)
	err := s.conn.Send("MULTI")
	if err != nil {
		return err
	}
	for key, value := range entries {
		err = s.conn.Send("SET", s.getPrefixedKey(key), value)
		if err != nil {
			return err
		}
	}
	_, err = s.conn.Do("EXEC")
	return err
}

// Delete deletes a value by key.
// It is implemented using the Redis DEL command.
// See https://redis.io/commands/del
func (s *Redis) Delete(key string) error {
	s.logger.Debugf("Redis Delete: %s", s.getPrefixedKey(key))
	s.deleteCounter.Inc(s.labelValues...)
	_, err := s.conn.Do("DEL", s.getPrefixedKey(key))
	return err
}

// Flush executes the SAVE command.
// See https://redis.io/commands/save
func (s *Redis) Flush() error {
	s.logger.Info("Redis Flush...")
	_, err := s.conn.Do("SAVE")
	s.logger.Info("Redis Flush complete")
	return err
}
