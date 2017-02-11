package kasper

import (
	"encoding/json"
	"log"
	"reflect"
)

type JsonSerde struct {
	value reflect.Value
}

func NewJsonSerde(witness interface{}) *JsonSerde {
	value := reflect.ValueOf(witness)
	if value.Kind() != reflect.Ptr {
		log.Fatal("Value must be a pointer type")
	}
	if value.Elem().Kind() != reflect.Struct {
		log.Fatal("JSON Serde witness must be a struct")
	}
	return &JsonSerde{value.Elem()}
}

func (serde *JsonSerde) Serialize(value interface{}) []byte {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Ptr {
		log.Fatal("Value must be a pointer type")
	}
	if v.Elem().Type() != serde.value.Type() {
		log.Fatal("Value struct type doesn't match given witness")
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		log.Fatalf("Could not serialize value to JSON: %#v", value)
	}
	return bytes
}

func (serde *JsonSerde) Deserialize(bytes []byte) interface{}  {
	value := reflect.New(serde.value.Type()).Interface()
	err := json.Unmarshal(bytes, &value)
	if err != nil {
		log.Fatal("Count not deserialize JSON byte array to struct")
	}
	return value
}
