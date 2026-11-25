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
	"io"
	"runtime"
	"strings"
	"sync"
	"unicode/utf8"
)

// ErrWriterClosed reports that a Writer is used after Close released it.
var ErrWriterClosed = errors.New("opendal: writer is closed")

// Write writes the given bytes to the specified path and returns the metadata
// of the written object (such as etag, version, or last modified) as reported
// by the underlying service.
//
// Write is a wrapper around the C-binding function
// `opendal_operator_write_with_cancel`. Which fields of the returned Metadata
// are populated depends on the service.
//
// # Parameters
//
//   - ctx: The context controlling cancellation and deadlines.
//   - path: The destination path where the bytes will be written.
//   - data: The byte slice containing the data to be written.
//   - opts: Optional write options.
//
// # Returns
//
//   - *Metadata: Metadata of the written object. Which fields are populated
//     depends on the service.
//   - error: An error if the write operation fails, or nil if successful.
//
// # Example
//
//	func exampleWrite(ctx context.Context, op *opendal.Operator) {
//		_, err := op.Write(ctx, "test", []byte("Hello opendal go binding!"))
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Write(ctx context.Context, path string, data []byte, opts ...WithWriteFn) (*Metadata, error) {
	inner, release, err := op.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	var cOpts *opendalWriteOptions
	var keepAlive writeOptionsKeepAlive
	if len(opts) > 0 {
		o := parseWriteOptions(opts...)
		cOpts, keepAlive, err = newOpendalWriteOptions(op.ctx, o)
		if err != nil {
			return nil, err
		}
		defer ffiWriteOptionsFree.symbol(op.ctx)(cOpts)
	}
	meta, err := withCancelTokenValue(op.ctx, ctx, func(token *opendalCancelToken) (*opendalMetadata, error) {
		return ffiOperatorWriteWithMetadata.symbol(op.ctx)(inner, path, data, cOpts, token)
	})
	runtime.KeepAlive(keepAlive)
	if err != nil {
		return nil, err
	}
	return newMetadata(op.ctx, meta), nil
}

// WithWriteFn is a functional option for write operations.
type WithWriteFn func(*writeOptions)

// WriteWithAppend sets append mode for the write operation.
func WriteWithAppend(append bool) WithWriteFn {
	return func(o *writeOptions) {
		o.append = append
	}
}

// WriteWithCacheControl sets the Cache-Control header for the write operation.
func WriteWithCacheControl(cacheControl string) WithWriteFn {
	return func(o *writeOptions) {
		o.cacheControl = cacheControl
	}
}

// WriteWithContentType sets the Content-Type header for the write operation.
func WriteWithContentType(contentType string) WithWriteFn {
	return func(o *writeOptions) {
		o.contentType = contentType
	}
}

// WriteWithContentDisposition sets the Content-Disposition header for the write operation.
func WriteWithContentDisposition(contentDisposition string) WithWriteFn {
	return func(o *writeOptions) {
		o.contentDisposition = contentDisposition
	}
}

// WriteWithContentEncoding sets the Content-Encoding header for the write operation.
func WriteWithContentEncoding(contentEncoding string) WithWriteFn {
	return func(o *writeOptions) {
		o.contentEncoding = contentEncoding
	}
}

// WriteWithUserMetadata sets user metadata for the write operation.
func WriteWithUserMetadata(userMetadata map[string]string) WithWriteFn {
	return func(o *writeOptions) {
		o.userMetadata = userMetadata
	}
}

// WriteWithIfMatch sets the If-Match condition for the write operation.
func WriteWithIfMatch(ifMatch string) WithWriteFn {
	return func(o *writeOptions) {
		o.ifMatch = ifMatch
	}
}

// WriteWithIfNoneMatch sets the If-None-Match condition for the write operation.
func WriteWithIfNoneMatch(ifNoneMatch string) WithWriteFn {
	return func(o *writeOptions) {
		o.ifNoneMatch = ifNoneMatch
	}
}

// WriteWithIfNotExists sets whether the write operation should only succeed if the target does not exist.
func WriteWithIfNotExists(ifNotExists bool) WithWriteFn {
	return func(o *writeOptions) {
		o.ifNotExists = ifNotExists
	}
}

// WriteWithConcurrent sets concurrent write operations.
func WriteWithConcurrent(concurrent uint) WithWriteFn {
	return func(o *writeOptions) {
		o.concurrent = concurrent
	}
}

// WriteWithChunk sets the chunk size for buffered writes.
func WriteWithChunk(chunk uint) WithWriteFn {
	return func(o *writeOptions) {
		o.chunk = chunk
	}
}

type writeOptions struct {
	append             bool
	cacheControl       string
	contentType        string
	contentDisposition string
	contentEncoding    string
	userMetadata       map[string]string
	ifMatch            string
	ifNoneMatch        string
	ifNotExists        bool
	concurrent         uint
	chunk              uint
}

func parseWriteOptions(opts ...WithWriteFn) *writeOptions {
	o := &writeOptions{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(o)
	}
	return o
}

type writeOptionsKeepAlive struct {
	strings      [][]byte
	userMetadata []opendalWriteUserMetadataPair
}

func newOpendalWriteOptions(ctx context.Context, o *writeOptions) (*opendalWriteOptions, writeOptionsKeepAlive, error) {
	cOpts := ffiWriteOptionsNew.symbol(ctx)()
	keepAlive := writeOptionsKeepAlive{}
	ffiWriteOptionsSetAppend.symbol(ctx)(cOpts, o.append)

	// fail frees the C-allocated options before returning
	fail := func(err error) (*opendalWriteOptions, writeOptionsKeepAlive, error) {
		ffiWriteOptionsFree.symbol(ctx)(cOpts)
		return nil, writeOptionsKeepAlive{}, err
	}

	setString := func(value string, set func(*opendalWriteOptions, string) ([]byte, error)) error {
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

	if err := setString(o.cacheControl, ffiWriteOptionsSetCacheControl.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.contentType, ffiWriteOptionsSetContentType.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.contentDisposition, ffiWriteOptionsSetContentDisposition.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.contentEncoding, ffiWriteOptionsSetContentEncoding.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.ifMatch, ffiWriteOptionsSetIfMatch.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.ifNoneMatch, ffiWriteOptionsSetIfNoneMatch.symbol(ctx)); err != nil {
		return fail(err)
	}

	ffiWriteOptionsSetIfNotExists.symbol(ctx)(cOpts, o.ifNotExists)
	if o.concurrent != 0 {
		ffiWriteOptionsSetConcurrent.symbol(ctx)(cOpts, o.concurrent)
	}
	if o.chunk != 0 {
		ffiWriteOptionsSetChunk.symbol(ctx)(cOpts, o.chunk)
	}
	if len(o.userMetadata) > 0 {
		keepAlive.userMetadata = make([]opendalWriteUserMetadataPair, 0, len(o.userMetadata))
		for key, value := range o.userMetadata {
			keyData, err := byteSliceFromString(key)
			if err != nil {
				return fail(err)
			}
			valueData, err := byteSliceFromString(value)
			if err != nil {
				return fail(err)
			}
			keepAlive.strings = append(keepAlive.strings, keyData, valueData)
			byteKey := &keyData[0]
			byteValue := &valueData[0]
			keepAlive.userMetadata = append(keepAlive.userMetadata, opendalWriteUserMetadataPair{key: byteKey, value: byteValue})
		}
		ffiWriteOptionsSetUserMetadata.symbol(ctx)(cOpts, keepAlive.userMetadata)
	}
	return cOpts, keepAlive, nil
}

// CreateDir creates a directory at the specified path.
//
// CreateDir is a wrapper around the C-binding function `opendal_operator_create_dir_with_cancel`.
// It provides a way to create directories in the storage system.
//
// # Parameters
//
//   - ctx: The context controlling cancellation and deadlines.
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
//	func exampleCreateDir(ctx context.Context, op *opendal.Operator) {
//		err := op.CreateDir(ctx, "test/")
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
// The trailing slash in "test/" is important to indicate it's a directory.
func (op *Operator) CreateDir(ctx context.Context, path string) error {
	inner, release, err := op.acquire()
	if err != nil {
		return err
	}
	defer release()
	return withCancelToken(op.ctx, ctx, func(token *opendalCancelToken) error {
		return ffiOperatorCreateDir.symbol(op.ctx)(inner, path, token)
	})
}

// Writer returns a new Writer for the specified path.
//
// Writer is a wrapper around the C-binding function `opendal_operator_writer_with_cancel`.
// It provides a way to obtain a writer for writing data to the storage system.
//
// # Parameters
//
//   - ctx: The context controlling cancellation and deadlines for opening the writer.
//   - path: The destination path where data will be written.
//   - opts: Optional write options.
//
// # Returns
//
//   - *Writer: A pointer to a Writer instance, or an error if the operation fails.
//
// # Example
//
//	func exampleWriter(ctx context.Context, op *opendal.Operator) {
//		writer, err := op.Writer(ctx, "test.txt")
//		if err != nil {
//			log.Fatal(err)
//		}
//		defer writer.Close(ctx)
//		_, err = writer.WriteContext(ctx, []byte("Hello opendal writer!"))
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Writer(ctx context.Context, path string, opts ...WithWriteFn) (*Writer, error) {
	opInner, release, err := op.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	var cOpts *opendalWriteOptions
	var keepAlive writeOptionsKeepAlive
	if len(opts) > 0 {
		o := parseWriteOptions(opts...)
		cOpts, keepAlive, err = newOpendalWriteOptions(op.ctx, o)
		if err != nil {
			return nil, err
		}
		defer ffiWriteOptionsFree.symbol(op.ctx)(cOpts)
	}
	inner, err := withCancelTokenValue(op.ctx, ctx, func(token *opendalCancelToken) (*opendalWriter, error) {
		return ffiOperatorWriter.symbol(op.ctx)(opInner, path, cOpts, token)
	})
	runtime.KeepAlive(keepAlive)
	if err != nil {
		return nil, err
	}
	return &Writer{inner: inner, ctx: op.ctx}, nil
}

const copyBufferSize = 256 * 1024

type Writer struct {
	// mu serializes access to inner. The C writer hands out a &mut to the
	// underlying Rust writer, so two goroutines calling in at once would alias
	// it, which is undefined behavior rather than merely a data race.
	mu    sync.Mutex
	inner *opendalWriter
	ctx   context.Context
}

// Write writes len(p) bytes from p to the underlying writer.
//
// This method implements the io.Writer interface for Writer.
// For cancellation and deadline control, use [Writer.WriteContext].
func (w *Writer) Write(p []byte) (n int, err error) {
	return w.WriteContext(context.Background(), p)
}

// WriteContext writes len(p) bytes from p to the underlying writer, cancelling
// the write if ctx is done before it completes.
func (w *Writer) WriteContext(ctx context.Context, p []byte) (n int, err error) {
	if w == nil {
		return 0, ErrWriterClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inner == nil {
		return 0, ErrWriterClosed
	}
	return withCancelTokenValue(w.ctx, ctx, func(token *opendalCancelToken) (int, error) {
		return ffiWriterWrite.symbol(w.ctx)(w.inner, p, token)
	})
}

// ReadFrom copies data from src to w using one 256 KiB buffer.
// It returns the number of bytes written and any error. Reading to EOF succeeds.
// ReadFrom leaves both streams open. Call [Writer.Close] to complete the write
// and get its metadata. ReadFrom on a closed Writer returns ErrWriterClosed
// without reading src.
func (w *Writer) ReadFrom(src io.Reader) (int64, error) {
	if w == nil {
		return 0, ErrWriterClosed
	}
	w.mu.Lock()
	closed := w.inner == nil
	w.mu.Unlock()
	if closed {
		return 0, ErrWriterClosed
	}

	buf := make([]byte, copyBufferSize)

	// Hide WriteTo and ReadFrom to prevent io.CopyBuffer from dispatching to them.
	reader := struct{ io.Reader }{Reader: src}
	writer := struct{ io.Writer }{Writer: w}
	return io.CopyBuffer(writer, reader, buf)
}

// Close finishes the write and releases the resources associated with the
// Writer, returning the metadata of the written object (such as etag, version,
// or last modified) as reported by the underlying service.
//
// It is important to call Close after writing all the data to ensure that the
// data is properly flushed and written to the storage. Otherwise, the data may
// be lost.
//
// Close is a wrapper around the C-binding function
// `opendal_writer_close_with_cancel`. Which fields of the returned Metadata
// are populated depends on the service.
//
// The first Close returns the write result; it releases the Writer even when
// the native close fails or ctx is cancelled, so a deferred call cannot leak
// the handle. Calling Close again is safe and returns ErrWriterClosed without
// touching the native library. Write, WriteContext, and ReadFrom on a closed
// Writer return ErrWriterClosed.
func (w *Writer) Close(ctx context.Context) (*Metadata, error) {
	if w == nil {
		return nil, ErrWriterClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inner == nil {
		return nil, ErrWriterClosed
	}
	inner := w.inner
	w.inner = nil
	defer ffiWriterFree.symbol(w.ctx)(inner)
	meta, err := withCancelTokenValue(w.ctx, ctx, func(token *opendalCancelToken) (*opendalMetadata, error) {
		return ffiWriterCloseWithMetadata.symbol(w.ctx)(inner, token)
	})
	if err != nil {
		return nil, err
	}
	return newMetadata(w.ctx, meta), nil
}

var _ io.Writer = (*Writer)(nil)
var _ io.ReaderFrom = (*Writer)(nil)

var ffiOperatorWriteWithMetadata = newFFI(
	"opendal_operator_write_with_cancel",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, bytes *opendalBytes, opts *opendalWriteOptions, cancel *opendalCancelToken, meta **opendalMetadata) *opendalError) func(op *opendalOperator, path string, data []byte, opts *opendalWriteOptions, cancel *opendalCancelToken) (*opendalMetadata, error) {
		return func(op *opendalOperator, path string, data []byte, opts *opendalWriteOptions, cancel *opendalCancelToken) (*opendalMetadata, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			bytes := toOpendalBytes(data)
			var meta *opendalMetadata
			e := cFunc(op, bytePath, bytes, opts, cancel, &meta)
			runtime.KeepAlive(bytePath)
			runtime.KeepAlive(data)
			runtime.KeepAlive(bytes)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return meta, nil
		}
	},
)

var ffiOperatorCreateDir = newFFI(
	"opendal_operator_create_dir_with_cancel",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, cancel *opendalCancelToken) *opendalError) func(op *opendalOperator, path string, cancel *opendalCancelToken) error {
		return func(op *opendalOperator, path string, cancel *opendalCancelToken) error {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return err
			}
			e := cFunc(op, bytePath, cancel)
			runtime.KeepAlive(bytePath)
			return parseError(ctx, e)
		}
	},
)

var ffiOperatorWriter = newFFI(
	"opendal_operator_writer_with_cancel",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, opts *opendalWriteOptions, cancel *opendalCancelToken, writer **opendalWriter) *opendalError) func(op *opendalOperator, path string, opts *opendalWriteOptions, cancel *opendalCancelToken) (*opendalWriter, error) {
		return func(op *opendalOperator, path string, opts *opendalWriteOptions, cancel *opendalCancelToken) (*opendalWriter, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var writer *opendalWriter
			e := cFunc(op, bytePath, opts, cancel, &writer)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return writer, nil
		}
	},
)

var ffiWriteOptionsNew = newDirectFFI[func() *opendalWriteOptions]("opendal_write_options_new")

var ffiWriteOptionsFree = newDirectFFI[func(opts *opendalWriteOptions)]("opendal_write_options_free")

var ffiWriteOptionsSetAppend = newDirectFFI[func(opts *opendalWriteOptions, append bool)]("opendal_write_options_set_append")

func byteSliceFromString(value string) ([]byte, error) {
	if strings.IndexByte(value, 0) >= 0 {
		return nil, errors.New("string contains nul")
	}
	if !utf8.ValidString(value) {
		return nil, errInvalidUTF8
	}
	return append([]byte(value), 0), nil
}

func newWriteOptionsSetStringFFI(sym string) *FFI[func(*opendalWriteOptions, string) ([]byte, error), func(*opendalWriteOptions, *byte)] {
	return newFFI(
		contextKey(sym),
		func(_ context.Context, cFunc func(opts *opendalWriteOptions, value *byte)) func(*opendalWriteOptions, string) ([]byte, error) {
			return func(opts *opendalWriteOptions, value string) ([]byte, error) {
				data, err := byteSliceFromString(value)
				if err != nil {
					return nil, err
				}
				cFunc(opts, &data[0])
				runtime.KeepAlive(data)
				return data, nil
			}
		},
	)
}

var ffiWriteOptionsSetCacheControl = newWriteOptionsSetStringFFI("opendal_write_options_set_cache_control")
var ffiWriteOptionsSetContentType = newWriteOptionsSetStringFFI("opendal_write_options_set_content_type")
var ffiWriteOptionsSetContentDisposition = newWriteOptionsSetStringFFI("opendal_write_options_set_content_disposition")
var ffiWriteOptionsSetContentEncoding = newWriteOptionsSetStringFFI("opendal_write_options_set_content_encoding")
var ffiWriteOptionsSetIfMatch = newWriteOptionsSetStringFFI("opendal_write_options_set_if_match")
var ffiWriteOptionsSetIfNoneMatch = newWriteOptionsSetStringFFI("opendal_write_options_set_if_none_match")

var ffiWriteOptionsSetIfNotExists = newDirectFFI[func(opts *opendalWriteOptions, ifNotExists bool)]("opendal_write_options_set_if_not_exists")

var ffiWriteOptionsSetConcurrent = newDirectFFI[func(opts *opendalWriteOptions, concurrent uint)]("opendal_write_options_set_concurrent")

var ffiWriteOptionsSetChunk = newDirectFFI[func(opts *opendalWriteOptions, chunk uint)]("opendal_write_options_set_chunk")

var ffiWriteOptionsSetUserMetadata = newFFI(
	"opendal_write_options_set_user_metadata",
	func(_ context.Context, cFunc func(opts *opendalWriteOptions, pairs *opendalWriteUserMetadataPair, length uint)) func(opts *opendalWriteOptions, userMetadata []opendalWriteUserMetadataPair) {
		return func(opts *opendalWriteOptions, userMetadata []opendalWriteUserMetadataPair) {
			var ptr *opendalWriteUserMetadataPair
			if len(userMetadata) > 0 {
				ptr = &userMetadata[0]
			}
			cFunc(opts, ptr, uint(len(userMetadata)))
			runtime.KeepAlive(userMetadata)
		}
	},
)

var ffiWriterFree = newDirectFFI[func(w *opendalWriter)]("opendal_writer_free")

var ffiWriterWrite = newFFI(
	"opendal_writer_write_with_cancel",
	func(ctx context.Context, cFunc func(w *opendalWriter, bytes *opendalBytes, cancel *opendalCancelToken, size *uint) *opendalError) func(w *opendalWriter, buf []byte, cancel *opendalCancelToken) (size int, err error) {
		return func(w *opendalWriter, buf []byte, cancel *opendalCancelToken) (size int, err error) {
			bytes := toOpendalBytes(buf)
			var outSize uint
			e := cFunc(w, bytes, cancel, &outSize)
			runtime.KeepAlive(buf)
			runtime.KeepAlive(bytes)
			if e != nil {
				return 0, parseError(ctx, e)
			}
			return int(outSize), nil
		}
	},
)

var ffiWriterCloseWithMetadata = newFFI(
	"opendal_writer_close_with_cancel",
	func(ctx context.Context, cFunc func(w *opendalWriter, cancel *opendalCancelToken, meta **opendalMetadata) *opendalError) func(w *opendalWriter, cancel *opendalCancelToken) (*opendalMetadata, error) {
		return func(w *opendalWriter, cancel *opendalCancelToken) (*opendalMetadata, error) {
			var meta *opendalMetadata
			e := cFunc(w, cancel, &meta)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return meta, nil
		}
	},
)
