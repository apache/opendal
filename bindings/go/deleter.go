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
	"errors"
	"runtime"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// ErrDeleterClosed reports that a Deleter is used after Close.
var ErrDeleterClosed = errors.New("opendal: deleter is closed")

// Deleter removes many paths from storage.
//
// Deleter queues paths and hands them to the service, using batch deletion when
// the service supports it. Delete only queues a path: the deletion may still be
// pending when Delete returns, and it is Close that flushes the queue and waits
// for every queued path to be removed. Treat a path as deleted only after Close
// returns nil.
//
// Prefer Operator.Delete for a single path; a Deleter pays off when removing
// many paths from a service with batch delete support, such as S3.
//
// # Example
//
//	func exampleDeleter(op *opendal.Operator) {
//		deleter, err := op.Deleter()
//		if err != nil {
//			log.Fatal(err)
//		}
//		for _, path := range []string{"a.txt", "b.txt", "c.txt"} {
//			if err := deleter.Delete(path); err != nil {
//				log.Fatal(err)
//			}
//		}
//		// Close reports whether the queued deletions succeeded.
//		if err := deleter.Close(); err != nil {
//			log.Fatal(err)
//		}
//	}
type Deleter struct {
	inner *opendalDeleter
	ctx   context.Context
}

// Deleter creates a Deleter that removes many paths.
//
// Deleter is a wrapper around the C-binding function `opendal_operator_deleter`.
//
// # Returns
//
//   - *Deleter: A Deleter used to queue deletions, or an error if the operation fails.
//
// # Behavior
//
//   - The returned Deleter must be released with Close, which also flushes the
//     queued deletions.
func (op *Operator) Deleter() (*Deleter, error) {
	inner, err := ffiOperatorDeleter.symbol(op.ctx)(op.inner)
	if err != nil {
		return nil, err
	}
	return &Deleter{inner: inner, ctx: op.ctx}, nil
}

// Delete queues path for deletion.
//
// The path is not necessarily removed when Delete returns: Close flushes the
// queue and waits for the deletions to complete. Delete returns
// ErrDeleterClosed once the Deleter is closed.
//
// # Parameters
//
//   - path: The path of the file or directory to delete.
//   - opts: Optional functional options, shared with Operator.Delete, to delete a
//     specific version or to delete recursively.
func (d *Deleter) Delete(path string, opts ...WithDeleteFn) error {
	if d.inner == nil {
		return ErrDeleterClosed
	}
	if len(opts) == 0 {
		return ffiDeleterDelete.symbol(d.ctx)(d.inner, path)
	}

	o := parseDeleteOptions(opts...)
	cOpts, keepAlive, err := newOpendalDeleteOptions(d.ctx, o)
	if err != nil {
		return err
	}
	defer ffiDeleteOptionsFree.symbol(d.ctx)(cOpts)
	err = ffiDeleterDeleteWith.symbol(d.ctx)(d.inner, path, cOpts)
	// cOpts holds raw pointers into the Go buffers tracked by keepAlive.
	// Keep them reachable until the native call above has returned.
	runtime.KeepAlive(keepAlive)
	return err
}

// Close flushes the queued deletions, waits for them to complete, and releases
// the resources held by the Deleter.
//
// The error reports whether the queued deletions succeeded, so callers that care
// about the outcome must check it rather than defer Close. The Deleter is
// released even when the flush fails, and further calls are no-ops that return
// nil.
func (d *Deleter) Close() error {
	if d.inner == nil {
		return nil
	}
	err := ffiDeleterClose.symbol(d.ctx)(d.inner)
	ffiDeleterFree.symbol(d.ctx)(d.inner)
	d.inner = nil
	return err
}

// Remove deletes the given paths.
//
// Remove drives a Deleter for the caller: it queues every path, then flushes and
// waits for the deletions. Services that support batch deletion remove the paths
// in batches.
//
// # Parameters
//
//   - paths: The paths to delete.
//
// # Returns
//
//   - error: An error if any deletion fails, or nil if all paths are removed.
//
// # Example
//
//	func exampleRemove(op *opendal.Operator) {
//		err := op.Remove([]string{"a.txt", "b.txt"})
//		if err != nil {
//			log.Printf("Remove operation failed: %v", err)
//		}
//	}
func (op *Operator) Remove(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	deleter, err := op.Deleter()
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := deleter.Delete(path); err != nil {
			// Close releases the deleter; the queued paths are dropped.
			_ = deleter.Close()
			return err
		}
	}
	return deleter.Close()
}

var ffiOperatorDeleter = newFFI(ffiOpts{
	sym:    "opendal_operator_deleter",
	rType:  &typeResultOperatorDeleter,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator) (*opendalDeleter, error) {
	return func(op *opendalOperator) (*opendalDeleter, error) {
		var result resultOperatorDeleter
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.deleter, nil
	}
})

var ffiDeleterDelete = newFFI(ffiOpts{
	sym:    "opendal_deleter_delete",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(d *opendalDeleter, path string) error {
	return func(d *opendalDeleter, path string) error {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return err
		}
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&d),
			unsafe.Pointer(&bytePath),
		)
		return parseError(ctx, e)
	}
})

var ffiDeleterDeleteWith = newFFI(ffiOpts{
	sym:    "opendal_deleter_delete_with",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(d *opendalDeleter, path string, opts *opendalDeleteOptions) error {
	return func(d *opendalDeleter, path string, opts *opendalDeleteOptions) error {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return err
		}
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&d),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&opts),
		)
		return parseError(ctx, e)
	}
})

var ffiDeleterClose = newFFI(ffiOpts{
	sym:    "opendal_deleter_close",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(d *opendalDeleter) error {
	return func(d *opendalDeleter) error {
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&d),
		)
		return parseError(ctx, e)
	}
})

var ffiDeleterFree = newFFI(ffiOpts{
	sym:    "opendal_deleter_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(d *opendalDeleter) {
	return func(d *opendalDeleter) {
		ffiCall(
			nil,
			unsafe.Pointer(&d),
		)
	}
})
