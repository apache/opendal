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
//		err := op.Delete("file.txt")
//		if err != nil {
//			log.Printf("Delete operation failed: %v", err)
//		}
//
//		// Delete with recursive option
//		err = op.Delete("dir/", opendal.DeleteWithRecursive(true))
//		if err != nil {
//			log.Printf("Delete operation failed: %v", err)
//		}
//
//		// Delete a specific version
//		err = op.Delete("file.txt", opendal.DeleteWithVersion("v1"))
//		if err != nil {
//			log.Printf("Delete operation failed: %v", err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Delete(path string, opts ...WithDeleteFn) error {
	if len(opts) == 0 {
		return ffiOperatorDelete.symbol(op.ctx)(op.inner, path)
	}

	o := parseDeleteOptions(opts...)
	cOpts, keepAlive, err := newOpendalDeleteOptions(op.ctx, o)
	if err != nil {
		return err
	}
	defer ffiDeleteOptionsFree.symbol(op.ctx)(cOpts)
	err = ffiOperatorDeleteWith.symbol(op.ctx)(op.inner, path, cOpts)
	runtime.KeepAlive(keepAlive)
	return err
}

func parseDeleteOptions(opts ...WithDeleteFn) *deleteOptions {
	o := &deleteOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func newOpendalDeleteOptions(ctx context.Context, o *deleteOptions) (*opendalDeleteOptions, [][]byte, error) {
	cOpts := ffiDeleteOptionsNew.symbol(ctx)()
	var keepAlive [][]byte

	fail := func(err error) (*opendalDeleteOptions, [][]byte, error) {
		ffiDeleteOptionsFree.symbol(ctx)(cOpts)
		return nil, nil, err
	}

	ffiDeleteOptionsSetRecursive.symbol(ctx)(cOpts, o.recursive)
	if o.version != nil {
		data, err := ffiDeleteOptionsSetVersion.symbol(ctx)(cOpts, *o.version)
		if err != nil {
			return fail(err)
		}
		keepAlive = append(keepAlive, data)
	}
	return cOpts, keepAlive, nil
}

var ffiOperatorDelete = newFFI(
	"opendal_operator_delete",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte) *opendalError) func(op *opendalOperator, path string) error {
		return func(op *opendalOperator, path string) error {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return err
			}
			e := cFunc(op, bytePath)
			runtime.KeepAlive(bytePath)
			return parseError(ctx, e)
		}
	},
)

var ffiDeleteOptionsNew = newDirectFFI[func() *opendalDeleteOptions]("opendal_delete_options_new")

var ffiDeleteOptionsSetVersion = newFFI(
	"opendal_delete_options_set_version",
	func(_ context.Context, cFunc func(opts *opendalDeleteOptions, version *byte)) func(opts *opendalDeleteOptions, version string) ([]byte, error) {
		return func(opts *opendalDeleteOptions, version string) ([]byte, error) {
			data, err := byteSliceFromString(version)
			if err != nil {
				return nil, err
			}
			cFunc(opts, &data[0])
			return data, nil
		}
	},
)

var ffiDeleteOptionsSetRecursive = newDirectFFI[func(opts *opendalDeleteOptions, recursive bool)]("opendal_delete_options_set_recursive")

var ffiDeleteOptionsFree = newDirectFFI[func(opts *opendalDeleteOptions)]("opendal_delete_options_free")

var ffiOperatorDeleteWith = newFFI(
	"opendal_operator_delete_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, opts *opendalDeleteOptions) *opendalError) func(op *opendalOperator, path string, opts *opendalDeleteOptions) error {
		return func(op *opendalOperator, path string, opts *opendalDeleteOptions) error {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return err
			}
			e := cFunc(op, bytePath, opts)
			runtime.KeepAlive(bytePath)
			return parseError(ctx, e)
		}
	},
)
