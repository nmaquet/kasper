package kasper

import "testing"

type Smurf struct {
	Age  int `json:"smurf_age"`
	Name string `json:"smurf_name"`
}

func TestJsonSerde_Serialize_Smurf(t *testing.T) {
	serde := NewJsonSerde(&Smurf{})
	s := serde.Serialize(&Smurf{Age: 245, Name: "Smurfette"})
	if string(s) != `{"smurf_age":245,"smurf_name":"Smurfette"}` {
		t.Fatal(string(s))
	}
}

func TestJsonSerde_Deserialize_Smurf(t *testing.T) {
	serde := NewJsonSerde(&Smurf{})
	expected := Smurf{Age: 245, Name: "Smurfette"}
	actual := serde.Deserialize([]byte(`{"smurf_age":245,"smurf_name":"Smurfette"}`)).(*Smurf)
	if *actual != expected {
		t.Fatal(actual)
	}
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