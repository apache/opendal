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
	"io"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// Write writes the given bytes to the specified path.
//
// Write is a wrapper around the C-binding function `opendal_operator_write`. It provides a simplified
// interface for writing data to the storage. Currently, this implementation does not support the
// `Operator::write_with` method from the original Rust library, nor does it support streaming writes
// or multipart uploads.
//
// # Parameters
//
//   - path: The destination path where the bytes will be written.
//   - data: The byte slice containing the data to be written.
//
// # Returns
//
//   - error: An error if the write operation fails, or nil if successful.
//
// # Example
//
//	func exampleWrite(op *opendal.Operator) {
//		err = op.Write("test", []byte("Hello opendal go binding!"))
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Write(path string, data []byte) error {
	return ffiOperatorWrite.symbol(op.ctx)(op.inner, path, data)
}

// CreateDir creates a directory at the specified path.
//
// CreateDir is a wrapper around the C-binding function `opendal_operator_create_dir`.
// It provides a way to create directories in the storage system.
//
// # Parameters
//
//   - path: The path where the directory should be created.
//
// # Returns
//
//   - error: An error if the directory creation fails, or nil if successful.
//
// # Notes
//
// It is mandatory to include a trailing slash (/) in the path to indicate
// that it is a directory. Failing to do so may result in a `CodeNotADirectory`
// error being returned by OpenDAL.
//
// # Behavior
//
//   - Creating a directory that already exists will succeed without error.
//   - Directory creation is always recursive, similar to the `mkdir -p` command.
//
// # Example
//
//	func exampleCreateDir(op *opendal.Operator) {
//		err = op.CreateDir("test/")
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
// The trailing slash in "test/" is important to indicate it's a directory.
func (op *Operator) CreateDir(path string) error {
	return ffiOperatorCreateDir.symbol(op.ctx)(op.inner, path)
}

// Writer returns a new Writer for the specified path.
//
// Writer is a wrapper around the C-binding function `opendal_operator_writer`.
// It provides a way to obtain a writer for writing data to the storage system.
//
// # Parameters
//
//   - path: The destination path where data will be written.
//
// # Returns
//
//   - *Writer: A pointer to a Writer instance, or an error if the operation fails.
//
// # Example
//
//	func exampleWriter(op *opendal.Operator) {
//		writer, err := op.Writer("test/")
//		if err != nil {
//			log.Fatal(err)
//		}
//		defer writer.Close()
//		_, err = writer.Write([]byte("Hello opendal writer!"))
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Writer(path string) (*Writer, error) {
	inner, err := ffiOperatorWriter.symbol(op.ctx)(op.inner, path)
	if err != nil {
		return nil, err
	}
	writer := &Writer{
		inner: inner,
		ctx:   op.ctx,
	}
	return writer, nil
}

type Writer struct {
	inner *opendalWriter
	ctx   context.Context
}

// Write writes the given bytes to the specified path.
//
// Write is a wrapper around the C-binding function `opendal_operator_write`. It provides a simplified
// interface for writing data to the storage. Write can be called multiple times to write
// additional data to the same path.
//
// The maximum size of the data that can be written in a single call is 256KB.
//
// # Parameters
//
//   - path: The destination path where the bytes will be written.
//   - data: The byte slice containing the data to be written.
//
// # Returns
//
//   - error: An error if the write operation fails, or nil if successful.
//
// # Example
//
//	func exampleWrite(op *opendal.Operator) {
//		err = op.Write("test", []byte("Hello opendal go binding!"))
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (w *Writer) Write(p []byte) (n int, err error) {
	return ffiWriterWrite.symbol(w.ctx)(w.inner, p)
}

// Close finishes the write and releases the resources associated with the Writer.
// It is important to call Close after writing all the data to ensure that the data is
// properly flushed and written to the storage. Otherwise, the data may be lost.
func (w *Writer) Close() error {
	defer ffiWriterFree.symbol(w.ctx)(w.inner)
	return ffiWriterClose.symbol(w.ctx)(w.inner)
}

var _ io.WriteCloser = (*Writer)(nil)

var ffiOperatorWrite = newFFI(ffiOpts{
	sym:    "opendal_operator_write",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string, data []byte) error {
	return func(op *opendalOperator, path string, data []byte) error {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return err
		}
		bytes := toOpendalBytes(data)
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&bytes),
		)
		return parseError(ctx, e)
	}
})

var ffiOperatorCreateDir = newFFI(ffiOpts{
	sym:    "opendal_operator_create_dir",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string) error {
	return func(op *opendalOperator, path string) error {
		bytePath, err := BytePtrFromString(path)
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

var ffiOperatorWriter = newFFI(ffiOpts{
	sym:    "opendal_operator_writer",
	rType:  &typeResultOperatorWriter,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string) (*opendalWriter, error) {
	return func(op *opendalOperator, path string) (*opendalWriter, error) {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultOperatorWriter
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.writer, nil
	}
})

var ffiWriterFree = newFFI(ffiOpts{
	sym:    "opendal_writer_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(w *opendalWriter) {
	return func(w *opendalWriter) {
		ffiCall(
			nil,
			unsafe.Pointer(&w),
		)
	}
})

var ffiWriterWrite = newFFI(ffiOpts{
	sym:    "opendal_writer_write",
	rType:  &typeResultWriterWrite,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(r *opendalWriter, buf []byte) (size int, err error) {
	return func(r *opendalWriter, buf []byte) (size int, err error) {
		bytes := toOpendalBytes(buf)
		var result resultWriterWrite
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&r),
			unsafe.Pointer(&bytes),
		)
		if result.error != nil {
			return 0, parseError(ctx, result.error)
		}
		return int(result.size), nil
	}
})

var ffiWriterClose = newFFI(ffiOpts{
	sym:    "opendal_writer_close",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(r *opendalWriter) error {
	return func(r *opendalWriter) error {
		var e *opendalError
		ffiCall(
			unsafe.Pointer(&e),
			unsafe.Pointer(&r),
		)
		return parseError(ctx, e)
	}
})
