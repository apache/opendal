// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This probe runs in CI with a copy of capability.go and the checked-in C header.
package main

/*
#include "opendal.h"
enum { capability_align = _Alignof(opendal_capability) };
*/
import "C"

import (
	"fmt"
	"os"
	"reflect"
	"strings"
)

func main() {
	if err := checkLayout(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func checkLayout() error {
	cLayout := reflect.TypeFor[C.opendal_capability]()
	goLayout := reflect.TypeFor[opendalCapability]()
	if goLayout.Size() != C.sizeof_opendal_capability || goLayout.Align() != C.capability_align {
		return fmt.Errorf("capability size/alignment: Go %d/%d, C %d/%d",
			goLayout.Size(), goLayout.Align(), C.sizeof_opendal_capability, C.capability_align)
	}
	goIndex := 0
	for cField := range cLayout.Fields() {
		if cField.Name == "_" {
			// cgo represents padding as blank fields.
			continue
		}
		if goIndex == goLayout.NumField() {
			return fmt.Errorf("C capability field %s has no Go field at index %d", cField.Name, goIndex)
		}
		goField := goLayout.Field(goIndex)
		var goType reflect.Type
		switch cField.Type {
		case reflect.TypeFor[C.bool]():
			goType = reflect.TypeFor[uint8]()
		case reflect.TypeFor[C.uintptr_t]():
			goType = reflect.TypeFor[uint]()
		default:
			return fmt.Errorf("unsupported C capability field %s: %s", cField.Name, cField.Type)
		}
		name := goFieldName(cField.Name)
		if goField.Name != name || goField.Type != goType || goField.Offset != cField.Offset {
			return fmt.Errorf("capability field %d (%s): Go %s %s at %d, want %s %s at %d",
				goIndex, cField.Name, goField.Name, goField.Type, goField.Offset,
				name, goType, cField.Offset)
		}
		goIndex++
	}
	if goIndex != goLayout.NumField() {
		return fmt.Errorf("capability field count: Go %d, C %d", goLayout.NumField(), goIndex)
	}
	return nil
}

func goFieldName(cName string) string {
	// cbindgen adds a trailing underscore to C++ keywords such as delete_.
	parts := strings.Split(strings.TrimSuffix(cName, "_"), "_")
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}
