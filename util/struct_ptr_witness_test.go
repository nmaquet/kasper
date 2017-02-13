package util

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

type Jedi struct {
	Name      string
	Darksider bool
}

type Stormtrooper struct{
	ID string
}

func TestStructPtrWitness_NewOK(t *testing.T) {
	NewStructPtrWitness(&Jedi{})
}


func TestStructPtrWitness_NewNotPtr(t *testing.T) {
	assert.Panics(t, func(){
		NewStructPtrWitness(Jedi{})
	})
}

func TestStructPtrWitness_NewNotStruct(t *testing.T) {
	x := 42
	assert.Panics(t, func(){
		NewStructPtrWitness(&x)
	})
}

func TestStructPtrWitness_Allocate(t *testing.T) {
	jediWitness := NewStructPtrWitness(&Jedi{})
	jedi := jediWitness.Allocate().(*Jedi)
	jedi.Name = "Yoda"
	jedi.Darksider = false
	assert.Equal(t, &Jedi{"Yoda", false}, jedi)
}

func TestStructPtrWitness_Nil(t *testing.T) {
	jediWitness := NewStructPtrWitness(&Jedi{})
	actual := jediWitness.Nil().(*Jedi)
	var expected *Jedi = nil
	assert.Equal(t, expected, actual)
}

func TestStructPtrWitness_AssertOk(t *testing.T) {
	jediWitness := NewStructPtrWitness(&Jedi{})
	jedi := &Jedi{"Anakin", true}
	jediWitness.Assert(jedi)
}

func TestStructPtrWitness_AssertNotOk(t *testing.T) {
	jediWitness := NewStructPtrWitness(&Jedi{})
	stormtrooper := &Stormtrooper{"FN-2187"}
	assert.Panics(t, func() {
		jediWitness.Assert(stormtrooper)
	})
}