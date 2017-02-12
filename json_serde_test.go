package kasper

import (
	"testing"
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
	s := serde.Serialize(&Smurf{Age: 245, Name: "Smurfette"})
	if string(s) != `{"smurf_age":245,"smurf_name":"Smurfette"}` {
		t.Fatal(string(s))
	}
}

func TestJsonSerde_Serialize_NonPointerValue(t *testing.T) {
	defer func() {
		if r := recover(); r != "Value must be a pointer type" {
			t.Fail()
		}
	}()
	serde := NewJsonSerde(&Smurf{})
	serde.Serialize(Smurf{Age: 245, Name: "Smurfette"})
}

func TestJsonSerde_Serialize_StructMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r != "Value struct type doesn't match witness" {
			t.Fail()
		}
	}()
	serde := NewJsonSerde(&Smurf{})
	serde.Serialize(&Wizard{Level: 17})
}

func TestJsonSerde_Deserialize_Smurf(t *testing.T) {
	serde := NewJsonSerde(&Smurf{})
	expected := Smurf{Age: 245, Name: "Smurfette"}
	actual := serde.Deserialize([]byte(`{"smurf_age":245,"smurf_name":"Smurfette"}`)).(*Smurf)
	if *actual != expected {
		t.Fatal(actual)
	}
}

func TestNewJsonSerde_NonPointerWitness(t *testing.T) {
	defer func() {
		if r := recover(); r != "Value must be a pointer type" {
			t.Fail()
		}
	}()
	NewJsonSerde(Smurf{})
}

func TestNewJsonSerde_NonStructWitness(t *testing.T) {
	defer func() {
		if r := recover(); r != "Witness must be a struct" {
			t.Fail()
		}
	}()
	x := 42
	NewJsonSerde(&x)
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