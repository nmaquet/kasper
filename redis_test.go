package kasper

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"testing"
)

var redisStore *Redis

func TestRedis_Get_Put(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Get non-existing key
	item, err := redisStore.Get("vorgansharax")
	assert.Nil(t, item)
	assert.Nil(t, err)

	// Put key
	err = redisStore.Put("vorgansharax", vorgansharax)
	assert.Nil(t, err)

	// Get key again, should find it this time
	dragon, err := redisStore.Get("vorgansharax")
	assert.Equal(t, vorgansharax, dragon)
}

func TestRedis_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Put key
	err := redisStore.Put("falkor", falkor)
	assert.Nil(t, err)

	// Delete key
	err = redisStore.Delete("falkor")
	assert.Nil(t, err)

	// Get key again, should not find it this time
	item, err := redisStore.Get("falkor")
	assert.Nil(t, err)
	assert.Nil(t, item)

	// Delete key again does nothing
	err = redisStore.Delete("falkor")
	assert.Nil(t, err)
}

func TestRedis_GetAll_PutAll(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// Put 3 keys
	err := redisStore.Put("saphira", saphira)
	assert.Nil(t, err)
	err = redisStore.Put("mushu", mushu)
	assert.Nil(t, err)
	err = redisStore.Put("fin-fang-foom", finFangFoom)
	assert.Nil(t, err)

	// GetAll on 4 keys, one non existing
	kvs, err := redisStore.GetAll([]string{
		"saphira",
		"draco",
		"mushu",
		"fin-fang-foom",
	})
	assert.Nil(t, err)

	// Check the 3 keys
	assert.Equal(t, 3, len(kvs))
	assert.Equal(t, saphira, kvs["saphira"])
	assert.Equal(t, mushu, kvs["mushu"])
	assert.Equal(t, finFangFoom, kvs["fin-fang-foom"])

	// Delete everything
	_, err = redisStore.conn.Do("FLUSHALL")
	assert.Nil(t, err)

	// PutAll all 3 dragons again
	err = redisStore.PutAll(kvs)
	assert.Nil(t, err)

	// Check the 3 keys once more
	kvs, err = redisStore.GetAll([]string{
		"saphira",
		"mushu",
		"fin-fang-foom",
	})
	assert.Nil(t, err)
	assert.Equal(t, 3, len(kvs))
	assert.Equal(t, saphira, kvs["saphira"])
	assert.Equal(t, mushu, kvs["mushu"])
	assert.Equal(t, finFangFoom, kvs["fin-fang-foom"])

	// GetAll of empty list should work
	_, err = redisStore.GetAll(nil)
	assert.Nil(t, err)
	_, err = redisStore.GetAll([]string{})
	assert.Nil(t, err)
}

func TestRedis_Flush(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	err := redisStore.Flush()
	assert.Nil(t, err)
}

func init() {
	if testing.Short() {
		return
	}
	config := &Config{
		TopicProcessorName: "test",
		Logger:             &noopLogger{},
		MetricsProvider:    &NoopMetricsProvider{},
	}
	url := fmt.Sprintf("redis://%s:6379", getCIHost())
	conn, err := redis.DialURL(url)
	if err != nil {
		panic(err)
	}
	redisStore = NewRedis(config, conn, "dragon")
	_, err = redisStore.conn.Do("FLUSHALL")
	if err != nil {
		panic(err)
	}
}
