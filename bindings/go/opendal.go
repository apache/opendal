package opendal

import "unsafe"

type Operator struct {
	op uintptr
}

func NewOperator(scheme string) *Operator {
	trick := append([]byte(scheme), 0)
	op := opendal_operator_new(uintptr(unsafe.Pointer(&trick[0])))

	return &Operator{
		op: op,
	}
}
