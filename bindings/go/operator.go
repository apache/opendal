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

// Copy duplicates a file from the source path to the destination path.
//
// This function copies the contents of the file at 'from' to a new or existing file at 'to'.
//
// # Parameters
//
//   - from: The source file path.
//   - to: The destination file path.
//
// # Returns
//
//   - error: An error if the copy operation fails, or nil if successful.
//
// # Behavior
//
//   - Both 'from' and 'to' must be file paths, not directories.
//   - If 'to' already exists, it will be overwritten.
//   - If 'from' and 'to' are identical, an 'IsSameFile' error will be returned.
//   - The copy operation is idempotent; repeated calls with the same parameters will yield the same result.
//
// # Example
//
//	func exampleCopy(op *operatorCopy) {
//		err = op.Copy("path/from/file", "path/to/file")
//		if err != nil {
//			log.Printf("Copy operation failed: %v", err)
//		} else {
//			log.Println("File copied successfully")
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Copy(src, dest string) error {
	cp := getFFI[operatorCopy](op.ctx, symOperatorCopy)
	return cp(op.inner, src, dest)
}

// Rename changes the name or location of a file from the source path to the destination path.
//
// This function moves a file from 'from' to 'to', effectively renaming or relocating it.
//
// # Parameters
//
//   - from: The current file path.
//   - to: The new file path.
//
// # Returns
//
//   - error: An error if the rename operation fails, or nil if successful.
//
// # Behavior
//
//   - Both 'from' and 'to' must be file paths, not directories.
//   - If 'to' already exists, it will be overwritten.
//   - If 'from' and 'to' are identical, an 'IsSameFile' error will be returned.
//
// # Example
//
//	func exampleRename(op *opendal.Operator) {
//		err = op.Rename("path/from/file", "path/to/file")
//		if err != nil {
//			log.Printf("Rename operation failed: %v", err)
//		} else {
//			log.Println("File renamed successfully")
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Rename(src, dest string) error {
	rename := getFFI[operatorRename](op.ctx, symOperatorRename)
	return rename(op.inner, src, dest)
}

const symOperatorNew = "opendal_operator_new"

type operatorNew func(scheme Scheme, opts *operatorOptions) (op *opendalOperator, err error)

var withOperatorNew = withFFI(ffiOpts{
	sym:    symOperatorNew,
	rType:  &typeResultOperatorNew,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorNew {
	return func(scheme Scheme, opts *operatorOptions) (op *opendalOperator, err error) {
		var byteName *byte
		byteName, err = unix.BytePtrFromString(scheme.Name())
		if err != nil {
			return
		}
		var result resultOperatorNew
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&byteName),
			unsafe.Pointer(&opts),
		)
		if result.error != nil {
			err = parseError(ctx, result.error)
			return
		}
		op = result.op
		return
	}
})

const symOperatorFree = "opendal_operator_free"

type operatorFree func(op *opendalOperator)

var withOperatorFree = withFFI(ffiOpts{
	sym:    symOperatorFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorFree {
	return func(op *opendalOperator) {
		ffiCall(
			nil,
			unsafe.Pointer(&op),
		)
	}
})

type operatorOptions struct{}

const symOperatorOptionsNew = "opendal_operator_options_new"

type operatorOptionsNew func() (opts *operatorOptions)

var withOperatorOptionsNew = withFFI(ffiOpts{
	sym:   symOperatorOptionsNew,
	rType: &ffi.TypePointer,
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorOptionsNew {
	return func() (opts *operatorOptions) {
		ffiCall(unsafe.Pointer(&opts))
		return
	}
})

const symOperatorOptionSet = "opendal_operator_options_set"

type operatorOptionsSet func(opts *operatorOptions, key, value string) error

var withOperatorOptionsSet = withFFI(ffiOpts{
	sym:    symOperatorOptionSet,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorOptionsSet {
	return func(opts *operatorOptions, key, value string) (err error) {
		var (
			byteKey   *byte
			byteValue *byte
		)
		byteKey, err = unix.BytePtrFromString(key)
		if err != nil {
			return err
		}
		byteValue, err = unix.BytePtrFromString(value)
		if err != nil {
			return err
		}
		ffiCall(
			nil,
			unsafe.Pointer(&opts),
			unsafe.Pointer(&byteKey),
			unsafe.Pointer(&byteValue),
		)
		return nil
	}
})

const symOperatorOptionsFree = "opendal_operator_options_free"

type operatorOptionsFree func(opts *operatorOptions)

var withOperatorOptionsFree = withFFI(ffiOpts{
	sym:    symOperatorOptionsFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorOptionsFree {
	return func(opts *operatorOptions) {
		ffiCall(
			nil,
			unsafe.Pointer(&opts),
		)
	}
})

const symOperatorCopy = "opendal_operator_copy"

type operatorCopy func(op *opendalOperator, src, dest string) (err error)

var withOperatorCopy = withFFI(ffiOpts{
	sym:    symOperatorCopy,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorCopy {
	return func(op *opendalOperator, src, dest string) (err error) {
		var (
			byteSrc  *byte
			byteDest *byte
		)
		byteSrc, err = unix.BytePtrFromString(src)
		if err != nil {
			return err
		}
		byteDest, err = unix.BytePtrFromString(dest)
		if err != nil {
			return err
		}
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&byteSrc),
			unsafe.Pointer(&byteDest),
		)
		return parseError(ctx, e)
	}
})

const symOperatorRename = "opendal_operator_rename"

type operatorRename func(op *opendalOperator, src, dest string) (err error)

var withOperatorRename = withFFI(ffiOpts{
	sym:    symOperatorRename,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorRename {
	return func(op *opendalOperator, src, dest string) (err error) {
		var (
			byteSrc  *byte
			byteDest *byte
		)
		byteSrc, err = unix.BytePtrFromString(src)
		if err != nil {
			return err
		}
		byteDest, err = unix.BytePtrFromString(dest)
		if err != nil {
			return err
		}
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&byteSrc),
			unsafe.Pointer(&byteDest),
		)
		return parseError(ctx, e)
	}
})

const symBytesFree = "opendal_bytes_free"

type bytesFree func(b *opendalBytes)

var withBytesFree = withFFI(ffiOpts{
	sym:    symBytesFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&typeBytes},
}, func(_ context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) bytesFree {
	return func(b *opendalBytes) {
		ffiCall(
			nil,
			unsafe.Pointer(&b),
		)
	}
})
