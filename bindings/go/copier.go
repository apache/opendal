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
	"runtime"
)

// Copier drives a long-running copy operation from a source to a destination.
//
// Unlike Copy, which performs the whole copy in a single call, a Copier lets the
// caller advance the copy step by step via Next, which is useful for reporting
// progress on large copies. The copy is complete once Next reports that no more
// progress is available. Always call Close to release the resources held by the
// Copier.
//
// # Example
//
//	func exampleCopier(op *opendal.Operator) {
//		copier, err := op.Copier("path/from/file", "path/to/file")
//		if err != nil {
//			log.Fatal(err)
//		}
//		defer copier.Close()
//		for {
//			n, ok, err := copier.Next()
//			if err != nil {
//				log.Fatal(err)
//			}
//			if !ok {
//				break
//			}
//			log.Printf("copied %d bytes", n)
//		}
//	}
type Copier struct {
	inner *opendalCopier
	ctx   context.Context
}

// Copier creates a Copier to copy a file from src to dest.
//
// Copier is a wrapper around the C-binding function `opendal_operator_copier`.
// When options are provided, it uses `opendal_operator_copier_with`.
//
// # Parameters
//
//   - src: The source file path.
//   - dest: The destination file path.
//   - opts: Optional copy options (shared with Copy).
//
// # Returns
//
//   - *Copier: A Copier used to drive the copy, or an error if the operation fails.
//
// # Behavior
//
//   - Both src and dest must be file paths, not directories.
//   - The returned Copier must be released with Close.
func (op *Operator) Copier(src, dest string, opts ...WithCopyFn) (*Copier, error) {
	if len(opts) == 0 {
		inner, err := ffiOperatorCopier.symbol(op.ctx)(op.inner, src, dest)
		if err != nil {
			return nil, err
		}
		return &Copier{inner: inner, ctx: op.ctx}, nil
	}

	o := parseCopyOptions(opts...)
	cOpts, keepAlive, err := newOpendalCopyOptions(op.ctx, o)
	if err != nil {
		return nil, err
	}
	defer ffiCopyOptionsFree.symbol(op.ctx)(cOpts)
	inner, err := ffiOperatorCopierWith.symbol(op.ctx)(op.inner, src, dest, cOpts)
	// cOpts holds raw pointers into the Go buffers tracked by keepAlive.
	// Keep them reachable until the native call above has returned.
	runtime.KeepAlive(keepAlive)
	if err != nil {
		return nil, err
	}
	return &Copier{inner: inner, ctx: op.ctx}, nil
}

// Next drives the copy forward by one step.
//
// It returns the number of bytes copied in this step. ok is false when the copy
// has completed; once that happens, further calls keep returning (0, false, nil).
// A non-nil error reports a failed copy. Calling Next on a closed Copier also
// returns (0, false, nil).
func (c *Copier) Next() (size uint, ok bool, err error) {
	if c.inner == nil {
		return 0, false, nil
	}
	return ffiCopierNext.symbol(c.ctx)(c.inner)
}

// Abort aborts the pending copy operation.
//
// After Abort, the Copier should still be released with Close. Calling Abort
// on a closed Copier is a no-op.
func (c *Copier) Abort() error {
	if c.inner == nil {
		return nil
	}
	return ffiCopierAbort.symbol(c.ctx)(c.inner)
}

// Close releases the resources held by the Copier. It must be called once the
// Copier is no longer needed. Close is safe to call more than once; subsequent
// calls are no-ops.
func (c *Copier) Close() error {
	if c.inner == nil {
		return nil
	}
	ffiCopierFree.symbol(c.ctx)(c.inner)
	c.inner = nil
	return nil
}

var ffiOperatorCopier = newFFI(
	"opendal_operator_copier",
	func(ctx context.Context, cFunc func(op *opendalOperator, src, dest *byte, copier **opendalCopier) *opendalError) func(op *opendalOperator, src, dest string) (*opendalCopier, error) {
		return func(op *opendalOperator, src, dest string) (*opendalCopier, error) {
			byteSrc, err := BytePtrFromString(src)
			if err != nil {
				return nil, err
			}
			byteDest, err := BytePtrFromString(dest)
			if err != nil {
				return nil, err
			}
			var copier *opendalCopier
			e := cFunc(op, byteSrc, byteDest, &copier)
			runtime.KeepAlive(byteSrc)
			runtime.KeepAlive(byteDest)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return copier, nil
		}
	},
)

var ffiOperatorCopierWith = newFFI(
	"opendal_operator_copier_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, src, dest *byte, opts *opendalCopyOptions, copier **opendalCopier) *opendalError) func(op *opendalOperator, src, dest string, opts *opendalCopyOptions) (*opendalCopier, error) {
		return func(op *opendalOperator, src, dest string, opts *opendalCopyOptions) (*opendalCopier, error) {
			byteSrc, err := BytePtrFromString(src)
			if err != nil {
				return nil, err
			}
			byteDest, err := BytePtrFromString(dest)
			if err != nil {
				return nil, err
			}
			var copier *opendalCopier
			e := cFunc(op, byteSrc, byteDest, opts, &copier)
			runtime.KeepAlive(byteSrc)
			runtime.KeepAlive(byteDest)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return copier, nil
		}
	},
)

var ffiCopierNext = newFFI(
	"opendal_copier_next",
	func(ctx context.Context, cFunc func(c *opendalCopier, size *uint, hasNext *bool) *opendalError) func(c *opendalCopier) (uint, bool, error) {
		return func(c *opendalCopier) (uint, bool, error) {
			var size uint
			var hasNext bool
			e := cFunc(c, &size, &hasNext)
			if e != nil {
				return 0, false, parseError(ctx, e)
			}
			return size, hasNext, nil
		}
	},
)

var ffiCopierAbort = newFFI(
	"opendal_copier_abort",
	func(ctx context.Context, cFunc func(c *opendalCopier) *opendalError) func(c *opendalCopier) error {
		return func(c *opendalCopier) error {
			e := cFunc(c)
			return parseError(ctx, e)
		}
	},
)

var ffiCopierFree = newDirectFFI[func(c *opendalCopier)]("opendal_copier_free")
