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

// Stat retrieves metadata for the specified path.
//
// This function is a wrapper around the C-binding function `opendal_operator_stat`.
//
// # Parameters
//
//   - path: The path of the file or directory to get metadata for.
//
// # Returns
//
//   - *Metadata: Metadata of the specified path.
//   - error: An error if the operation fails, or nil if successful.
//
// # Notes
//
//   - The current implementation does not support `stat_with` functionality.
//   - If the path does not exist, an error with code opendal.CodeNotFound will be returned.
//
// # Example
//
//	func exampleStat(op *opendal.Operator) {
//		meta, err := op.Stat("/path/to/file")
//		if err != nil {
//			if e, ok := err.(*opendal.Error); ok && e.Code() == opendal.CodeNotFound {
//				fmt.Println("File not found")
//				return
//			}
//			log.Fatalf("Stat operation failed: %v", err)
//		}
//		fmt.Printf("File size: %d bytes\n", meta.ContentLength())
//		fmt.Printf("Last modified: %v\n", meta.LastModified())
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Stat(path string) (*Metadata, error) {
	stat := getFFI[operatorStat](op.ctx, symOperatorStat)
	meta, err := stat(op.inner, path)
	if err != nil {
		return nil, err
	}
	return newMetadata(op.ctx, meta), nil
}

// IsExist checks if a file or directory exists at the specified path.
//
// This method provides a convenient way to determine the existence of a resource
// without fetching its full metadata.
//
// # Parameters
//
//   - path: The path of the file or directory to check.
//
// # Returns
//
//   - bool: true if the resource exists, false otherwise.
//   - error: An error if the check operation fails, or nil if the check is successful.
//     Note that a false return value with a nil error indicates that the resource does not exist.
//
// # Example
//
//	exists, err := op.IsExist("path/to/file")
//	if err != nil {
//		log.Fatalf("Error checking existence: %v", err)
//	}
//	if exists {
//		fmt.Println("The file exists")
//	} else {
//		fmt.Println("The file does not exist")
//	}
func (op *Operator) IsExist(path string) (bool, error) {
	isExist := getFFI[operatorIsExist](op.ctx, symOperatorIsExist)
	return isExist(op.inner, path)
}

const symOperatorStat = "opendal_operator_stat"

type operatorStat func(op *opendalOperator, path string) (*opendalMetadata, error)

var withOperatorStat = withFFI(ffiOpts{
	sym:    symOperatorStat,
	rType:  &typeResultStat,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorStat {
	return func(op *opendalOperator, path string) (*opendalMetadata, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultStat
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.meta, nil
	}
})

const symOperatorIsExist = "opendal_operator_is_exist"

type operatorIsExist func(op *opendalOperator, path string) (bool, error)

var withOperatorIsExists = withFFI(ffiOpts{
	sym:    symOperatorIsExist,
	rType:  &typeResultIsExist,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorIsExist {
	return func(op *opendalOperator, path string) (bool, error) {
		bytePath, err := unix.BytePtrFromString(path)
		if err != nil {
			return false, err
		}
		var result resultIsExist
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return false, parseError(ctx, result.error)
		}
		return result.is_exist == 1, nil
	}
})
