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

// WithCopyFn is a functional option for copy operations.
type WithCopyFn func(*copyOptions)

// CopyWithIfNotExists sets whether the copy operation should only succeed
// if the target does not exist.
func CopyWithIfNotExists(ifNotExists bool) WithCopyFn {
	return func(o *copyOptions) {
		o.ifNotExists = ifNotExists
	}
}

// CopyWithIfMatch sets the If-Match condition for the copy operation.
//
// The copy will only succeed when the destination object's ETag matches
// the given value.
func CopyWithIfMatch(ifMatch string) WithCopyFn {
	return func(o *copyOptions) {
		o.ifMatch = ifMatch
	}
}

// CopyWithSourceVersion sets the source version for the copy operation.
//
// The copy will read from the specified source version instead of the
// current source object.
func CopyWithSourceVersion(sourceVersion string) WithCopyFn {
	return func(o *copyOptions) {
		o.sourceVersion = sourceVersion
	}
}

// CopyWithSourceContentLengthHint provides a hint of the source object's
// content length, avoiding extra metadata requests.
func CopyWithSourceContentLengthHint(length uint64) WithCopyFn {
	return func(o *copyOptions) {
		o.sourceContentLengthHint = length
	}
}

// CopyWithConcurrent sets the maximum number of concurrent copy operations.
func CopyWithConcurrent(concurrent uint) WithCopyFn {
	return func(o *copyOptions) {
		o.concurrent = concurrent
	}
}

// CopyWithChunk sets the chunk size for segmented copy operations.
func CopyWithChunk(chunk uint) WithCopyFn {
	return func(o *copyOptions) {
		o.chunk = chunk
	}
}

type copyOptions struct {
	ifNotExists             bool
	ifMatch                 string
	sourceVersion           string
	sourceContentLengthHint uint64
	concurrent              uint
	chunk                   uint
}

// Copy duplicates a file from the source path to the destination path.
//
// Copy is a wrapper around the C-binding function `opendal_operator_copy`.
// When options are provided, it uses `opendal_operator_copy_with`.
//
// # Parameters
//
//   - src: The source file path.
//   - dest: The destination file path.
//   - opts: Optional copy options.
//
// # Returns
//
//   - error: An error if the copy operation fails, or nil if successful.
//
// # Behavior
//
//   - Both src and dest must be file paths, not directories.
//   - If dest already exists, it will be overwritten.
//   - If src and dest are identical, an IsSameFile error will be returned.
//   - The copy operation is idempotent; repeated calls with the same parameters will yield the same result.
//
// # Example
//
//	func exampleCopy(op *opendal.Operator) {
//		err := op.Copy("path/from/file", "path/to/file")
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Copy(src, dest string, opts ...WithCopyFn) error {
	if len(opts) == 0 {
		return ffiOperatorCopy.symbol(op.ctx)(op.inner, src, dest)
	}

	o := parseCopyOptions(opts...)
	cOpts, keepAlive, err := newOpendalCopyOptions(op.ctx, o)
	if err != nil {
		return err
	}
	defer ffiCopyOptionsFree.symbol(op.ctx)(cOpts)
	err = ffiOperatorCopyWith.symbol(op.ctx)(op.inner, src, dest, cOpts)
	// cOpts holds raw pointers into the Go buffers tracked by keepAlive; keep
	// them reachable until the native call above has returned.
	runtime.KeepAlive(keepAlive)
	return err
}

func parseCopyOptions(opts ...WithCopyFn) *copyOptions {
	o := &copyOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// copyOptionsKeepAlive holds byte slices that must outlive the FFI call that
// references them, since the C side borrows the underlying memory.
type copyOptionsKeepAlive struct {
	strings [][]byte
}

// newOpendalCopyOptions builds a C-side opendal_copy_options from o. On error it
// frees the allocated options before returning. The caller is responsible for
// freeing the returned options and for keeping keepAlive reachable until after
// the FFI call that consumes the options.
func newOpendalCopyOptions(ctx context.Context, o *copyOptions) (*opendalCopyOptions, copyOptionsKeepAlive, error) {
	cOpts := ffiCopyOptionsNew.symbol(ctx)()
	keepAlive := copyOptionsKeepAlive{}

	fail := func(err error) (*opendalCopyOptions, copyOptionsKeepAlive, error) {
		ffiCopyOptionsFree.symbol(ctx)(cOpts)
		return nil, copyOptionsKeepAlive{}, err
	}

	setString := func(value string, set func(*opendalCopyOptions, string) ([]byte, error)) error {
		if value == "" {
			return nil
		}
		data, err := set(cOpts, value)
		if err != nil {
			return err
		}
		keepAlive.strings = append(keepAlive.strings, data)
		return nil
	}

	ffiCopyOptionsSetIfNotExists.symbol(ctx)(cOpts, o.ifNotExists)
	if err := setString(o.ifMatch, ffiCopyOptionsSetIfMatch.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.sourceVersion, ffiCopyOptionsSetSourceVersion.symbol(ctx)); err != nil {
		return fail(err)
	}
	if o.sourceContentLengthHint != 0 {
		ffiCopyOptionsSetSourceContentLengthHint.symbol(ctx)(cOpts, o.sourceContentLengthHint)
	}
	if o.concurrent != 0 {
		ffiCopyOptionsSetConcurrent.symbol(ctx)(cOpts, o.concurrent)
	}
	if o.chunk != 0 {
		ffiCopyOptionsSetChunk.symbol(ctx)(cOpts, o.chunk)
	}
	return cOpts, keepAlive, nil
}

var ffiCopyOptionsNew = newDirectFFI[func() *opendalCopyOptions]("opendal_copy_options_new")

var ffiCopyOptionsFree = newDirectFFI[func(opts *opendalCopyOptions)]("opendal_copy_options_free")

var ffiCopyOptionsSetIfNotExists = newDirectFFI[func(opts *opendalCopyOptions, ifNotExists bool)]("opendal_copy_options_set_if_not_exists")

var ffiCopyOptionsSetIfMatch = newFFI(
	"opendal_copy_options_set_if_match",
	func(_ context.Context, cFunc func(opts *opendalCopyOptions, ifMatch *byte)) func(opts *opendalCopyOptions, ifMatch string) ([]byte, error) {
		return func(opts *opendalCopyOptions, ifMatch string) ([]byte, error) {
			data, err := byteSliceFromString(ifMatch)
			if err != nil {
				return nil, err
			}
			cFunc(opts, &data[0])
			return data, nil
		}
	},
)

var ffiCopyOptionsSetSourceVersion = newFFI(
	"opendal_copy_options_set_source_version",
	func(_ context.Context, cFunc func(opts *opendalCopyOptions, sourceVersion *byte)) func(opts *opendalCopyOptions, sourceVersion string) ([]byte, error) {
		return func(opts *opendalCopyOptions, sourceVersion string) ([]byte, error) {
			data, err := byteSliceFromString(sourceVersion)
			if err != nil {
				return nil, err
			}
			cFunc(opts, &data[0])
			return data, nil
		}
	},
)

var ffiCopyOptionsSetSourceContentLengthHint = newDirectFFI[func(opts *opendalCopyOptions, sourceContentLengthHint uint64)]("opendal_copy_options_set_source_content_length_hint")

var ffiCopyOptionsSetConcurrent = newDirectFFI[func(opts *opendalCopyOptions, concurrent uint)]("opendal_copy_options_set_concurrent")

var ffiCopyOptionsSetChunk = newDirectFFI[func(opts *opendalCopyOptions, chunk uint)]("opendal_copy_options_set_chunk")

var ffiOperatorCopyWith = newFFI(
	"opendal_operator_copy_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, src, dest *byte, opts *opendalCopyOptions) *opendalError) func(op *opendalOperator, src, dest string, opts *opendalCopyOptions) error {
		return func(op *opendalOperator, src, dest string, opts *opendalCopyOptions) error {
			byteSrc, err := BytePtrFromString(src)
			if err != nil {
				return err
			}
			byteDest, err := BytePtrFromString(dest)
			if err != nil {
				return err
			}
			e := cFunc(op, byteSrc, byteDest, opts)
			runtime.KeepAlive(byteSrc)
			runtime.KeepAlive(byteDest)
			return parseError(ctx, e)
		}
	},
)
