/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package opendal

/*
#include "opendal.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

var (
	errInvalidScheme = errors.New("invalid scheme")
	errValueEmpty    = errors.New("value is empty")
)

type Options map[string]string

type Operator struct {
	inner *C.opendal_operator
}

func NewOperator(scheme string, opt Options) (*Operator, error) {
	if len(scheme) == 0 {
		return nil, errInvalidScheme
	}
	opts := C.opendal_operator_options_new()
	defer C.opendal_operator_options_free(opts)
	for k, v := range opt {
		C.opendal_operator_options_set(opts, C.CString(k), C.CString(v))
	}
	ret := C.opendal_operator_new(C.CString(scheme), opts)
	if ret.error != nil {
		defer C.opendal_error_free(ret.error)
		code, message := parseError(ret.error)
		return nil, errors.New(fmt.Sprintf("create operator failed, error code: %d, error message: %s", code, message))
	}
	return &Operator{
		inner: ret.op,
	}, nil
}

func (o *Operator) Write(key string, value []byte) error {
	if len(value) == 0 {
		return errValueEmpty
	}
	bytes := C.opendal_bytes{data: (*C.uchar)(unsafe.Pointer(&value[0])), len: C.ulong(len(value))}
	ret := C.opendal_operator_write(o.inner, C.CString(key), bytes)
	if ret != nil {
		defer C.opendal_error_free(ret)
		code, message := parseError(ret)
		return errors.New(fmt.Sprintf("write failed, error code: %d, error message: %s", code, message))
	}
	return nil
}

func (o *Operator) Read(key string) ([]byte, error) {
	ret := C.opendal_operator_read(o.inner, C.CString(key))
	if ret.error != nil {
		defer C.opendal_error_free(ret.error)
		code, message := parseError(ret.error)
		return nil, errors.New(fmt.Sprintf("read failed, error code: %d, error message: %s", code, message))
	}
	return C.GoBytes(unsafe.Pointer(ret.data.data), C.int(ret.data.len)), nil
}

func (o *Operator) Close() error {
	C.opendal_operator_free(o.inner)
	return nil
}

func decodeBytes(bs C.opendal_bytes) []byte {
	return C.GoBytes(unsafe.Pointer(bs.data), C.int(bs.len))
}

func parseError(err *C.opendal_error) (int, string) {
	code := int(err.code)
	message := string(decodeBytes(err.message))

	return code, message
}
