package kasper

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

type Smurf struct {
	Age  int `json:"smurf_age"`
	Name string `json:"smurf_name"`
}

type Wizard struct {
	Level int
	Nemesis *Wizard
}

func TestJsonSerde_Serialize_Smurf(t *testing.T) {
	serde := NewJsonSerde(&Smurf{})
	actual := serde.Serialize(&Smurf{Age: 245, Name: "Smurfette"})
	expected := `{"smurf_age":245,"smurf_name":"Smurfette"}`
	assert.Equal(t, expected, string(actual))
}

func TestJsonSerde_Serialize_NonPointerValue(t *testing.T) {
	assert.Panics(t, func() {
		serde := NewJsonSerde(&Smurf{})
		serde.Serialize(Smurf{Age: 245, Name: "Smurfette"})
	})
}

func TestJsonSerde_Serialize_StructMismatch(t *testing.T) {
	assert.Panics(t, func() {
		serde := NewJsonSerde(&Smurf{})
		serde.Serialize(&Wizard{Level: 17})
	})
}

func TestJsonSerde_Deserialize_Smurf(t *testing.T) {
	serde := NewJsonSerde(&Smurf{})
	expected := &Smurf{Age: 245, Name: "Smurfette"}
	actual := serde.Deserialize([]byte(`{"smurf_age":245,"smurf_name":"Smurfette"}`)).(*Smurf)
	assert.Equal(t, expected, actual)
}

func TestNewJsonSerde_NonPointerWitness(t *testing.T) {
	assert.Panics(t, func() {
		NewJsonSerde(Smurf{})
	})
}

func TestNewJsonSerde_NonStructWitness(t *testing.T) {
	assert.Panics(t, func(){
		x := 42
		NewJsonSerde(&x)
	})
}

func BenchmarkJsonSerde_Serialize(b *testing.B) {
	serde := NewJsonSerde(&Smurf{})
	for i := 0; i < b.N; i++ {
		serde.Serialize(&Smurf{Age: 245, Name: "Smurfette"})
	}
}

func BenchmarkJsonSerde_Deserialize(b *testing.B) {
	serde := NewJsonSerde(&Smurf{})
	for i := 0; i < b.N; i++ {
		serde.Deserialize([]byte(`{"smurf_age":245,"smurf_name":"Smurfette"}`))
	}
}