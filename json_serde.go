package kasper

import (
	"encoding/json"
	"github.com/movio/kasper/util"
)

// JSONSerde serializes and deserializes structs using JSON type descriptions
type JSONSerde struct {
	witness *util.StructPtrWitness
}

// NewJSONSerde creates a serde for given witness
func NewJSONSerde(structPtr interface{}) *JSONSerde {
	witness := util.NewStructPtrWitness(structPtr)
	return &JSONSerde{witness}
}

// Serialize returns serialized value as a byte array
func (serde *JSONSerde) Serialize(structPtr interface{}) []byte {
	serde.witness.Assert(structPtr)
	bytes, err := json.Marshal(structPtr)
	if err != nil {
		panic(err)
	}
	return bytes
}

// Deserialize returns a struct deserialized from byte array
func (serde *JSONSerde) Deserialize(bytes []byte) interface{} {
	structPtr := serde.witness.Allocate()
	err := json.Unmarshal(bytes, structPtr)
	if err != nil {
		panic(err)
	}
	return structPtr
}
