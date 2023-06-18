package opendal

import (
	"github.com/ebitengine/purego"
)

var OPENDAL_LIB uintptr

var opendal_operator_options_new func() uintptr
var opendal_operator_new func(uintptr) uintptr

func init() {
	var err error
	OPENDAL_LIB, err = openLibrary()
	if err != nil {
		panic(err)
	}

	purego.RegisterLibFunc(&opendal_operator_options_new, OPENDAL_LIB, "opendal_operator_options_new")
	purego.RegisterLibFunc(&opendal_operator_new, OPENDAL_LIB, "opendal_operator_new")
}
