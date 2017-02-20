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
	response, err := s.conn.Do("GET", s.getPrefixedKey(key))
	if err != nil {
		return s.witness.Nil(), err
	}
	if response == nil {
		return s.witness.Nil(), nil
	}
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
	_, err = s.conn.Do("SET", s.getPrefixedKey(key), bytes)
	return err
}

func (s *RedisKeyValueStore) PutAll(entries []*Entry) error {
	err := s.conn.Send("MULTI")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		s.witness.Assert(entry.value)
		var bytes []byte
		bytes, err = json.Marshal(entry.value)
		if err != nil {
			return err
		}
		err = s.conn.Send("SET", s.getPrefixedKey(entry.key), bytes)
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
