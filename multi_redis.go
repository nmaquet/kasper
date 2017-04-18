package kasper

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"sort"
)

// MultiRedis is an implementation of MultiStore that uses Redis.
// Each instance provides multitenant key-value access to keys of the form {tenant}/{keyPrefix}/{key}.
// This implementation uses Gary Burd's Go Redis client.
// See https://github.com/garyburd/redigo
type MultiRedis struct {
	config    *Config
	conn      redis.Conn
	stores    map[string]Store
	keyPrefix string

	logger       Logger
	labelValues  []string
	pushCounter  Counter
	fetchCounter Counter
}

// NewMultiRedis creates MultiRedis instances.
// All keys read and written will be of the form:
//	{tenant}/{keyPrefix}/{key}
func NewMultiRedis(config *Config, conn redis.Conn, keyPrefix string) *MultiRedis {
	metrics := config.MetricsProvider
	labelNames := []string{"topicProcessor", "keyPrefix"}
	s := &MultiRedis{
		config,
		conn,
		make(map[string]Store),
		keyPrefix,
		config.Logger,
		[]string{config.TopicProcessorName, keyPrefix},
		metrics.NewCounter("MultiRedis_Push", "Counter of Push() calls", labelNames...),
		metrics.NewCounter("MultiRedis_Fetch", "Counter of Fetch() calls", labelNames...),
	}
	return s
}

func (s *MultiRedis) getPrefixedKey(tenant string, key string) string {
	return fmt.Sprintf("%s/%s/%s", tenant, s.keyPrefix, key)
}

// Tenant returns an Redis Store for the given tenant.
// Created instances are cached on future invocations.
func (s *MultiRedis) Tenant(tenant string) Store {
	tenantKeyPrefix := fmt.Sprintf("%s/%s", tenant, s.keyPrefix)
	kv, found := s.stores[tenant]
	if !found {
		kv = NewRedis(s.config, s.conn, tenantKeyPrefix)
		s.stores[tenant] = kv
	}
	return kv
}

// AllTenants returns the list of tenants known to this instance.
func (s *MultiRedis) AllTenants() []string {
	tenants := make([]string, len(s.stores))
	i := 0
	for tenant := range s.stores {
		tenants[i] = tenant
		i++
	}
	sort.Strings(tenants)
	return tenants
}

// Fetch performs a single MULTI GET Redis command across multiple tenants.
func (s *MultiRedis) Fetch(keys []TenantKey) (*MultiMap, error) {
	s.fetchCounter.Inc(s.labelValues...)
	res := NewMultiMap(len(keys) / 10)
	if len(keys) == 0 {
		return res, nil
	}
	err := s.conn.Send("MULTI")
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		err = s.conn.Send("GET", s.getPrefixedKey(key.Tenant, key.Key))
		if err != nil {
			return nil, err
		}
	}
	values, err := redis.Values(s.conn.Do("EXEC"))
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		bytes, err := redis.Bytes(value, err)
		if err != nil {
			return nil, err
		}
		err = res.Tenant(keys[i].Tenant).Put(keys[i].Key, bytes)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Fetch performs a single MULTI SET Redis command across multiple tenants.
func (s *MultiRedis) Push(entries *MultiMap) error {
	s.pushCounter.Inc(s.labelValues...)
	err := s.conn.Send("MULTI")
	if err != nil {
		return err
	}
	for _, tenant := range entries.AllTenants() {
		for key, value := range entries.Tenant(tenant).(*Map).GetMap() {
			err = s.conn.Send("SET", s.getPrefixedKey(tenant, key), value)
			if err != nil {
				return err
			}
		}
	}
	_, err = s.conn.Do("EXEC")
	return err
}
