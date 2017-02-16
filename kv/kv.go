/*

Kasper companion library for stateful stream processing.

*/

package kv

type Entry struct {
	key   string
	value interface{}
}

// KeyValueStore is universal interface for a key-value store
// Keys are strings, and values are pointers to structs
type KeyValueStore interface {
	Get(key string) (interface{}, error)
	Put(key string, value interface{}) error
	PutAll(entries []*Entry) error
	Delete(key string) error
	Flush() error
}
