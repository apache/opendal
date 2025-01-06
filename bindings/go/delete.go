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

import (
	"context"
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

// Delete removes the file or directory at the specified path.
//
// # Parameters
//
//   - path: The path of the file or directory to delete.
//
// # Returns
//
//   - error: An error if the deletion fails, or nil if successful.
//
// # Note
//
// Use with caution as this operation is irreversible.
func (op *Operator) Delete(path string) error {
	delete := getFFI[operatorDelete](op.ctx, symOperatorDelete)
	return delete(op.inner, path)
}

type operatorDelete func(op *opendalOperator, path string) error

const symOperatorDelete = "opendal_operator_delete"

var withOperatorDelete = withFFI(ffiOpts{
	sym:    symOperatorDelete,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorDelete {
	return func(op *opendalOperator, path string) error {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return err
		}
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		return parseError(ctx, e)
	}
})
