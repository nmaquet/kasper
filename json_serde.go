package kasper

import (
	"encoding/json"
	"reflect"
)

type JSONSerde struct {
	value reflect.Value
}

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

func (serde *JSONSerde) Deserialize(bytes []byte) interface{} {
	value := reflect.New(serde.value.Type()).Interface()
	err := json.Unmarshal(bytes, &value)
	if err != nil {
		panic(err)
	}
	return value
}
