package kv

import (
	"encoding/json"

	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/movio/kasper/util"
)

type RedisKeyValueStore struct {
	conn      redis.Conn
	keyPrefix string
	witness   *util.StructPtrWitness
}

func NewRedisKeyValueStore(url string, keyPrefix string, structPtr interface{}) (*RedisKeyValueStore, error) {
	conn, err := redis.DialURL(url)
	if err != nil {
		return nil, err
	}
	return &RedisKeyValueStore{conn, keyPrefix, util.NewStructPtrWitness(structPtr)}, nil
}

func (s *RedisKeyValueStore) getPrefixedKey(key string) string {
	return fmt.Sprintf("%s%s", s.keyPrefix, key)
}

func (s *RedisKeyValueStore) Get(key string) (interface{}, error) {
	value, err := s.conn.Do("GET", s.getPrefixedKey(key))
	if err != nil {
		return s.witness.Nil(), err
	}
	if value == nil {
		return s.witness.Nil(), nil
	}
	bytes, err := redis.Bytes(value, err)
	if err != nil {
		return s.witness.Nil(), err
	}
	structPtr := s.witness.Allocate()
	err = json.Unmarshal(bytes, structPtr)
	if err != nil {
		return s.witness.Nil(), err
	}
	return structPtr, err
}

func (s *RedisKeyValueStore) GetAll(keys []string) ([]*Entry, error) {
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
	entries := make([]*Entry, len(keys))
	for i, value := range values {
		bytes, err := redis.Bytes(value, err)
		if err != nil {
			return nil, err
		}
		structPtr := s.witness.Allocate()
		err = json.Unmarshal(bytes, structPtr)
		if err != nil {
			return nil, err
		}
		entries[i] = &Entry{keys[i], bytes }
	}
	return entries, nil
}

func (s *RedisKeyValueStore) Put(key string, structPtr interface{}) error {
	s.witness.Assert(structPtr)
	bytes, err := json.Marshal(structPtr)
	if err != nil {
		return err
	}
	_, err = s.conn.Do("SET", s.getPrefixedKey(key), bytes)
	return err
}

func (s *RedisKeyValueStore) PutAll(entries []*Entry) error {
	err := s.conn.Send("MULTI")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		s.witness.Assert(entry.Value)
		var bytes []byte
		bytes, err = json.Marshal(entry.Value)
		if err != nil {
			return err
		}
		err = s.conn.Send("SET", s.getPrefixedKey(entry.Key), bytes)
		if err != nil {
			return err
		}
	}
	_, err = s.conn.Do("EXEC")
	return err
}

func (s *RedisKeyValueStore) Delete(key string) error {
	_, err := s.conn.Do("DEL", s.getPrefixedKey(key))
	return err
}

func (s *RedisKeyValueStore) Flush() error {
	_, err := s.conn.Do("SAVE")
	return err
}
