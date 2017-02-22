/*

Kasper companion library for stateful stream processing.

*/

package kv

// Entry is a key-value pair for KeyValueStore
type Entry struct {
	Key   string
	Value interface{}
}

// KeyValueStore is universal interface for a key-value store
// Keys are strings, and values are pointers to structs
type KeyValueStore interface {
	Get(key string) (interface{}, error)
	GetAll(keys []string) ([]*Entry, error)
	Put(key string, value interface{}) error
	PutAll(entries []*Entry) error
	Delete(key string) error
	Flush() error
}

func ToMap(entries []*Entry, err error) (map[string]interface{}, error) {
	if err != nil {
		return nil, err
	}
	res := make(map[string] interface{}, len(entries))
	for _, entry := range entries {
		if entry.Value != nil {
			res[entry.Key] = entry.Value
		}
	}
	return res, nil
}

func FromMap(m map[string]interface{}) []*Entry {
	res := make([]*Entry, len(m))
	i := 0
	for key, value := range m {
		if value == nil {
			continue
		}
		res[i] = &Entry{key, value}
		i++
	}
	return res[0:i]
}
