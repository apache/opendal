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
)

// WithDeleteFn is a functional option for the Delete operation.
type WithDeleteFn func(*deleteOptions)

// DeleteWithVersion sets the version for the delete operation.
//
// When version is set, only the specified version of the object will be deleted.
// This is useful for versioned storage backends such as S3 versioning or GCS object versioning.
func DeleteWithVersion(version string) WithDeleteFn {
	return func(o *deleteOptions) {
		o.version = &version
	}
}

// DeleteWithRecursive sets the recursive flag for the delete operation.
//
// When recursive is true, all entries under the path (or sharing the prefix for
// file-like paths) will be removed.
func DeleteWithRecursive(recursive bool) WithDeleteFn {
	return func(o *deleteOptions) {
		o.recursive = recursive
	}
}

// deleteOptions holds the options for a delete operation.
type deleteOptions struct {
	version   *string
	recursive bool
}

// Delete removes the file or directory at the specified path.
//
// # Parameters
//
//   - ctx: The context for the operation. Canceling it cancels the underlying
//     native call in a blocking manner.
//   - path: The path of the file or directory to delete.
//   - opts: Optional functional options to configure the delete operation.
//
// # Returns
//
//   - error: An error if the deletion fails, or nil if successful.
//
// # Example
//
//	func exampleDelete(op *opendal.Operator) {
//		// Delete without options
//		err := op.Delete(context.Background(), "file.txt")
//		if err != nil {
//			log.Printf("Delete operation failed: %v", err)
//		}
//
//		// Delete with recursive option
//		err = op.Delete(context.Background(), "dir/", opendal.DeleteWithRecursive(true))
//		if err != nil {
//			log.Printf("Delete operation failed: %v", err)
//		}
//
//		// Delete a specific version
//		err = op.Delete(context.Background(), "file.txt", opendal.DeleteWithVersion("v1"))
//		if err != nil {
//			log.Printf("Delete operation failed: %v", err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Delete(ctx context.Context, path string, opts ...WithDeleteFn) error {
	return runErrWithCancelContext(ctx, op.ctx, func(token *opendalCancelToken) error {
		if len(opts) == 0 {
			return ffiOperatorDeleteWithCancel.symbol(op.ctx)(op.inner, path, token)
		}
		o := &deleteOptions{}
		for _, opt := range opts {
			opt(o)
		}
		cOpts := ffiDeleteOptionsNew.symbol(op.ctx)()
		defer ffiDeleteOptionsFree.symbol(op.ctx)(cOpts)
		ffiDeleteOptionsSetRecursive.symbol(op.ctx)(cOpts, o.recursive)
		if o.version != nil {
			ffiDeleteOptionsSetVersion.symbol(op.ctx)(cOpts, *o.version)
		}
		return ffiOperatorDeleteWithOptionsCancel.symbol(op.ctx)(op.inner, path, cOpts, token)
	})
}

var ffiOperatorDeleteWithCancel = newFFI(ffiOpts{
	sym:    "opendal_operator_delete_with_cancel",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string, token *opendalCancelToken) error {
	return func(op *opendalOperator, path string, token *opendalCancelToken) error {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return err
		}
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&token),
		)
		return parseError(ctx, e)
	}
})

var ffiDeleteOptionsNew = newFFI(ffiOpts{
	sym:   "opendal_delete_options_new",
	rType: &ffi.TypePointer,
}, func(_ context.Context, ffiCall ffiCall) func() *opendalDeleteOptions {
	return func() *opendalDeleteOptions {
		var opts *opendalDeleteOptions
		ffiCall(unsafe.Pointer(&opts))
		return opts
	}
})

var ffiDeleteOptionsSetVersion = newFFI(ffiOpts{
	sym:    "opendal_delete_options_set_version",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalDeleteOptions, version string) {
	return func(opts *opendalDeleteOptions, version string) {
		bytePtr, err := BytePtrFromString(version)
		if err != nil {
			return
		}
		ffiCall(
			nil,
			unsafe.Pointer(&opts),
			unsafe.Pointer(&bytePtr),
		)
	}
})

var ffiDeleteOptionsSetRecursive = newFFI(ffiOpts{
	sym:    "opendal_delete_options_set_recursive",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypeUint8},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalDeleteOptions, recursive bool) {
	return func(opts *opendalDeleteOptions, recursive bool) {
		var r uint8
		if recursive {
			r = 1
		}
		ffiCall(
			nil,
			unsafe.Pointer(&opts),
			unsafe.Pointer(&r),
		)
	}
})

var ffiDeleteOptionsFree = newFFI(ffiOpts{
	sym:    "opendal_delete_options_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalDeleteOptions) {
	return func(opts *opendalDeleteOptions) {
		ffiCall(
			nil,
			unsafe.Pointer(&opts),
		)
	}
})

var ffiOperatorDeleteWithOptionsCancel = newFFI(ffiOpts{
	sym:    "opendal_operator_delete_with_options_cancel",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string, opts *opendalDeleteOptions, token *opendalCancelToken) error {
	return func(op *opendalOperator, path string, opts *opendalDeleteOptions, token *opendalCancelToken) error {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return err
		}
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&opts),
			unsafe.Pointer(&token),
		)
		return parseError(ctx, e)
	}
})
