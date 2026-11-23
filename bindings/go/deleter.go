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

package opendal

import (
	"context"
	"errors"
	"runtime"
	"sync"
)

// ErrDeleterClosed reports that a Deleter is used after it is released by Close
// or Discard.
var ErrDeleterClosed = errors.New("opendal: deleter is closed")

// Deleter removes many paths from storage.
//
// Deleter uses batch deletion when the service supports it. Delete may remove
// paths immediately or trigger a batch. Call Flush or Close to wait for pending
// deletions to complete.
//
// Operator.WithDeleter creates and releases a Deleter for the caller. Callers
// that create a Deleter directly must release it with Close or Discard.
//
// A Deleter is safe for concurrent use: its methods serialize against each
// other, so a delete never overlaps a flush or a release.
//
// # Example
//
//	func exampleDeleter(op *opendal.Operator) {
//		err := op.WithDeleter(func(d *opendal.Deleter) error {
//			for _, path := range []string{"a.txt", "b.txt", "c.txt"} {
//				if err := d.Delete(path); err != nil {
//					return err
//				}
//			}
//			return nil
//		})
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
type Deleter struct {
	// mu serializes access to inner. The C deleter hands out a &mut to the
	// underlying Rust deleter, so two goroutines calling in at once would alias
	// it, which is undefined behavior rather than merely a data race.
	mu    sync.Mutex
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
//   - The returned Deleter must be released, either with Close, which flushes the
//     queued deletions first, or with Discard, which drops them.
//   - Prefer Operator.WithDeleter, which pairs the release with the queueing for
//     the caller.
func (op *Operator) Deleter() (*Deleter, error) {
	opInner, release, err := op.acquire()
	if err != nil {
		return nil, err
	}
	inner, err := ffiOperatorDeleter.symbol(op.ctx)(opInner)
	release()
	if err != nil {
		return nil, err
	}
	return &Deleter{inner: inner, ctx: op.ctx}, nil
}

// Delete submits path for deletion.
//
// Call Flush or Close to complete pending deletions. Delete returns
// ErrDeleterClosed once the Deleter is released.
//
// # Parameters
//
//   - path: The path of the file or directory to delete.
//   - opts: Optional functional options, shared with Operator.Delete, to delete a
//     specific version or to delete recursively.
func (d *Deleter) Delete(path string, opts ...WithDeleteFn) error {
	if d == nil {
		return ErrDeleterClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
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
	runtime.KeepAlive(keepAlive)
	return err
}

// deleteMany queues every path in one crossing of the FFI boundary.
//
// Queueing paths one at a time costs a boundary crossing and a blocking call per
// path, which dominates on services whose queueing is just a buffer insert. This
// hands the whole slice over at once. Every path is queued with default options,
// so callers that need per-path options must use Delete.
func (d *Deleter) deleteMany(paths []string) error {
	if d == nil {
		return ErrDeleterClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inner == nil {
		return ErrDeleterClosed
	}
	// An empty slice is handled by the FFI wrapper and the C side; Remove, the
	// only caller, already skips creating a deleter for it.
	return ffiDeleterDeleteMany.symbol(d.ctx)(d.inner, paths)
}

// Flush hands the queued deletions to the service, waits for them to complete,
// and keeps the Deleter usable.
//
// If a batch flush fails, pending paths remain queued for a retry. Flush
// returns ErrDeleterClosed once the Deleter is released.
func (d *Deleter) Flush() error {
	if d == nil {
		return ErrDeleterClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inner == nil {
		return ErrDeleterClosed
	}
	return ffiDeleterFlush.symbol(d.ctx)(d.inner)
}

// Close flushes the queued deletions, waits for them to complete, and releases
// the resources held by the Deleter.
//
// Close always releases the Deleter and returns the flush error, if any. Use
// Flush first if you need to retry a failed batch. Further calls to Close
// return nil.
func (d *Deleter) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inner == nil {
		return nil
	}
	defer d.releaseLocked()
	return ffiDeleterFlush.symbol(d.ctx)(d.inner)
}

// Discard releases the Deleter without flushing, dropping every path that is
// still queued.
//
// Use Discard to abandon queued deletions, and to release a Deleter on a path
// where flushing is not wanted. Discarding does not undo the deletions the
// service already performed. Discard is safe to call more than once, and after
// Close; further calls are no-ops.
func (d *Deleter) Discard() {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inner == nil {
		return
	}
	d.releaseLocked()
}

// releaseLocked frees the native deleter and blocks further use. The caller must
// hold d.mu and must have checked that d.inner is not nil.
func (d *Deleter) releaseLocked() {
	ffiDeleterFree.symbol(d.ctx)(d.inner)
	d.inner = nil
}

// WithDeleter runs fn with a Deleter and releases it before returning.
//
// When fn returns nil, WithDeleter flushes pending deletions and returns the
// flush result. When fn returns an error or panics, WithDeleter releases the
// Deleter without flushing. Earlier deletions may already have completed, for
// example when Delete runs immediately or fills a batch. The Deleter is valid
// only for the duration of fn.
//
// # Parameters
//
//   - fn: The function that queues paths on the Deleter.
//
// # Returns
//
//   - error: The error from fn, from creating the Deleter, or from the flush.
//
// # Example
//
//	func exampleWithDeleter(op *opendal.Operator) {
//		err := op.WithDeleter(func(d *opendal.Deleter) error {
//			return d.Delete("a.txt", opendal.DeleteWithVersion("v1"))
//		})
//		if err != nil {
//			log.Printf("delete failed: %v", err)
//		}
//	}
func (op *Operator) WithDeleter(fn func(d *Deleter) error) error {
	deleter, err := op.Deleter()
	if err != nil {
		return err
	}
	// Discard is a no-op once Close has released the deleter, and unlike the
	// straight-line calls it also runs when fn panics.
	defer deleter.Discard()
	if err := fn(deleter); err != nil {
		return err
	}
	return deleter.Close()
}

// Remove deletes the given paths.
//
// Remove drives a Deleter for the caller: it queues every path, then flushes and
// waits for the deletions. Services that support batch deletion remove the paths
// in batches.
//
// Remove queues the whole slice in one crossing of the FFI boundary rather than
// one per path.
//
// Submission stops at the first error. Earlier paths may already have been
// deleted. Remove uses default options for every path. Use WithDeleter for
// per-path options.
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
	return op.WithDeleter(func(d *Deleter) error {
		return d.deleteMany(paths)
	})
}

var ffiOperatorDeleter = newFFI(
	"opendal_operator_deleter",
	func(ctx context.Context, cFunc func(op *opendalOperator, deleter **opendalDeleter) *opendalError) func(op *opendalOperator) (*opendalDeleter, error) {
		return func(op *opendalOperator) (*opendalDeleter, error) {
			var deleter *opendalDeleter
			if err := cFunc(op, &deleter); err != nil {
				return nil, parseError(ctx, err)
			}
			return deleter, nil
		}
	},
)

var ffiDeleterDelete = newFFI(
	"opendal_deleter_delete",
	func(ctx context.Context, cFunc func(d *opendalDeleter, path *byte) *opendalError) func(d *opendalDeleter, path string) error {
		return func(d *opendalDeleter, path string) error {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return err
			}
			e := cFunc(d, bytePath)
			runtime.KeepAlive(bytePath)
			return parseError(ctx, e)
		}
	},
)

var ffiDeleterDeleteMany = newFFI(
	"opendal_deleter_delete_many",
	func(ctx context.Context, cFunc func(d *opendalDeleter, paths **byte, pathsLen uint) *opendalError) func(d *opendalDeleter, paths []string) error {
		return func(d *opendalDeleter, paths []string) error {
			pathPointers := make([]*byte, len(paths))
			for i, path := range paths {
				ptr, err := BytePtrFromString(path)
				if err != nil {
					return err
				}
				pathPointers[i] = ptr
			}

			var pathsPtr **byte
			if len(pathPointers) > 0 {
				pathsPtr = &pathPointers[0]
			}
			e := cFunc(d, pathsPtr, uint(len(pathPointers)))
			// The pointer array also keeps each path buffer alive during the call.
			runtime.KeepAlive(pathPointers)
			return parseError(ctx, e)
		}
	},
)

var ffiDeleterDeleteWith = newFFI(
	"opendal_deleter_delete_with",
	func(ctx context.Context, cFunc func(d *opendalDeleter, path *byte, opts *opendalDeleteOptions) *opendalError) func(d *opendalDeleter, path string, opts *opendalDeleteOptions) error {
		return func(d *opendalDeleter, path string, opts *opendalDeleteOptions) error {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return err
			}
			e := cFunc(d, bytePath, opts)
			runtime.KeepAlive(bytePath)
			return parseError(ctx, e)
		}
	},
)

var ffiDeleterFlush = newFFI(
	"opendal_deleter_flush",
	func(ctx context.Context, cFunc func(d *opendalDeleter) *opendalError) func(d *opendalDeleter) error {
		return func(d *opendalDeleter) error {
			return parseError(ctx, cFunc(d))
		}
	},
)

var ffiDeleterFree = newDirectFFI[func(d *opendalDeleter)]("opendal_deleter_free")
