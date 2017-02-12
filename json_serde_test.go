package kasper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Smurf struct {
	Age  int    `json:"smurf_age"`
	Name string `json:"smurf_name"`
}

type Wizard struct {
	Level   int
	Nemesis *Wizard
}

func TestJSONSerde_Serialize_Smurf(t *testing.T) {
	serde := NewJSONSerde(&Smurf{})
	actual := serde.Serialize(&Smurf{Age: 245, Name: "Smurfette"})
	expected := `{"smurf_age":245,"smurf_name":"Smurfette"}`
	assert.Equal(t, expected, string(actual))
}

func TestJSONSerde_Serialize_NonPointerValue(t *testing.T) {
	assert.Panics(t, func() {
		serde := NewJSONSerde(&Smurf{})
		serde.Serialize(Smurf{Age: 245, Name: "Smurfette"})
	})
}

func TestJSONSerde_Serialize_StructMismatch(t *testing.T) {
	assert.Panics(t, func() {
		serde := NewJSONSerde(&Smurf{})
		serde.Serialize(&Wizard{Level: 17})
	})
}

func TestJSONSerde_Deserialize_Smurf(t *testing.T) {
	serde := NewJSONSerde(&Smurf{})
	expected := &Smurf{Age: 245, Name: "Smurfette"}
	actual := serde.Deserialize([]byte(`{"smurf_age":245,"smurf_name":"Smurfette"}`)).(*Smurf)
	assert.Equal(t, expected, actual)
}

func TestNewJSONSerde_NonPointerWitness(t *testing.T) {
	assert.Panics(t, func() {
		NewJSONSerde(Smurf{})
	})
}

func TestNewJSONSerde_NonStructWitness(t *testing.T) {
	assert.Panics(t, func() {
		x := 42
		NewJSONSerde(&x)
	})
}

func BenchmarkJSONSerde_Serialize(b *testing.B) {
	serde := NewJSONSerde(&Smurf{})
	for i := 0; i < b.N; i++ {
		serde.Serialize(&Smurf{Age: 245, Name: "Smurfette"})
	}
}

func BenchmarkJSONSerde_Deserialize(b *testing.B) {
	serde := NewJSONSerde(&Smurf{})
	for i := 0; i < b.N; i++ {
		serde.Deserialize([]byte(`{"smurf_age":245,"smurf_name":"Smurfette"}`))
	}
}
