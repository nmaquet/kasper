package kasper

import (
	"encoding/json"
	"reflect"
)

// JSONSerde serializes and deserializes structs using JSON type descriptions
type JSONSerde struct {
	value reflect.Value
}

// NewJSONSerde creates a serde for given witness.
func NewJSONSerde(witness interface{}) *JSONSerde {
	value := reflect.ValueOf(witness)
	if value.Kind() != reflect.Ptr {
		panic("Value must be a pointer type")
	}
	if value.Elem().Kind() != reflect.Struct {
		panic("Witness must be a struct")
	}
	return &JSONSerde{value.Elem()}
}

// Serialize returns serialized value as a byte array
func (serde *JSONSerde) Serialize(value interface{}) []byte {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr {
		panic("Value must be a pointer type")
	}
	if v.Elem().Type() != serde.value.Type() {
		panic("Value struct type doesn't match witness")
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return bytes
}

// Deserialize returns a struct deserialized from byte array
func (serde *JSONSerde) Deserialize(bytes []byte) interface{} {
	value := reflect.New(serde.value.Type()).Interface()
	err := json.Unmarshal(bytes, &value)
	if err != nil {
		panic(err)
	}
	return value
}
