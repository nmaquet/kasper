package kasper

import "reflect"

type structPtrWitness struct {
	name string
	structValue reflect.Value
}

func newStructPtrWitness(structPtr interface{}) *structPtrWitness {
	value := reflect.ValueOf(structPtr)
	if value.Kind() != reflect.Ptr {
		log.Panic("Value must be a pointer type")
	}
	if value.Elem().Kind() != reflect.Struct {
		log.Panic("Witness must be a struct")
	}
	return &structPtrWitness{value.Elem().Type().Name(), value.Elem() }
}

func (w *structPtrWitness) assert(structPtr interface{}) {
	structPtrValue := reflect.ValueOf(structPtr)
	if structPtrValue.Kind() != reflect.Ptr {
		log.Panic("Value must be a pointer type")
	}
	if structPtrValue.Elem().Type() != w.structValue.Type() {
		log.Panic("Value struct type doesn't match witnessed type")
	}
}

func (w *structPtrWitness) allocate() interface{} {
	value := reflect.New(w.structValue.Type()).Interface()
	return value
}

func (w *structPtrWitness) nil() interface{} {
	value := reflect.Zero(reflect.PtrTo(w.structValue.Type())).Interface()
	return value
}
