package kv

import (
	"github.com/garyburd/redigo/redis"
	"github.com/movio/kasper/util"
	"encoding/json"
)

type RedisKeyValueStore struct {
	conn    redis.Conn
	witness *util.StructPtrWitness
}

func NewRedisKeyValueStore(url string, structPtr interface{}) (*RedisKeyValueStore, error) {
	conn, err := redis.DialURL(url)
	if err != nil {
		return nil, err
	}
	return &RedisKeyValueStore{conn, util.NewStructPtrWitness(structPtr)}, nil
}

func (s *RedisKeyValueStore) Get(key string) (interface{}, error) {
	response, err := s.conn.Do("GET", key)
	bytes, err := redis.Bytes(response, err)
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

func (s *RedisKeyValueStore) Put(key string, structPtr interface{}) error {
	s.witness.Assert(structPtr)
	bytes, err := json.Marshal(structPtr)
	if err != nil {
		return err
	}
	_, err = s.conn.Do("PUT", key, bytes)
	return err
}

func (s *RedisKeyValueStore) PutAll(entries []*Entry) error {
	s.conn.Send("MULTI")
	for _, entry := range entries {
		s.witness.Assert(entry.value)
		bytes, err := json.Marshal(entry.value)
		if err != nil {
			return err
		}
		err = s.conn.Send("PUT", entry.key, bytes)
		if err != nil {
			return err
		}
	}
	_, err := s.conn.Do("EXEC")
	return err
}

func (s *RedisKeyValueStore) Delete(key string) error {
	_, err := s.conn.Do("DEL", key)
	return err
}

func (s *RedisKeyValueStore) Flush() error {
	_, err := s.conn.Do("SAVE")
	return err
}
