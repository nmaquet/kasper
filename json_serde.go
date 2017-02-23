package kasper

import (
	"encoding/json"
)

// JSONSerde serializes and deserializes structs using JSON type descriptions
type JSONSerde struct {
	witness *structPtrWitness
}

// NewJSONSerde creates a serde for given witness
func NewJSONSerde(structPtr interface{}) *JSONSerde {
	witness := newStructPtrWitness(structPtr)
	return &JSONSerde{witness}
}

// Serialize returns serialized value as a byte array
func (serde *JSONSerde) Serialize(structPtr interface{}) []byte {
	serde.witness.assert(structPtr)
	bytes, err := json.Marshal(structPtr)
	if err != nil {
		panic(err)
	}
	return bytes
}

// Deserialize returns a struct deserialized from byte array
func (serde *JSONSerde) Deserialize(bytes []byte) interface{} {
	structPtr := serde.witness.allocate()
	err := json.Unmarshal(bytes, structPtr)
	if err != nil {
		panic(err)
	}
	return structPtr
}
