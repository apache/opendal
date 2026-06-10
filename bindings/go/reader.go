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
	"runtime"
	"time"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// Read reads the entire contents of the file at the specified path into a byte slice.
//
// Read is a wrapper around the C-binding function `opendal_operator_read`.
// When options are provided, it uses `opendal_operator_read_with`.
//
// # Parameters
//
//   - ctx: The context for the operation. When the context is canceled or its
//     deadline is exceeded, the underlying native read is canceled and Read
//     returns ctx.Err() after the native call has stopped.
//   - path: The path of the file to read.
//   - opts: Optional read options.
//
// # Returns
//
//   - []byte: The contents of the file as a byte slice.
//   - error: An error if the read operation fails, or nil if successful.
//
// # Notes
//
//   - Read allocates a new byte slice internally. For more precise memory control
//     or lazy reading, consider using the Reader() method instead.
//
// # Example
//
//	func exampleRead(op *opendal.Operator) {
//		data, err := op.Read(context.Background(), "test")
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("Read: %s\n", data)
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Read(ctx context.Context, path string, opts ...WithReadFn) ([]byte, error) {
	bytes, err := runWithCancelContext(ctx, op.ctx, func(token *opendalCancelToken) (opendalBytes, error) {
		return op.readWithCancel(path, token, opts...)
	}, func(bytes opendalBytes) {
		if bytes.data != nil {
			ffiBytesFree.symbol(op.ctx)(&bytes)
		}
	})
	if err != nil {
		return nil, err
	}

	data := parseBytes(bytes)
	if len(data) > 0 {
		ffiBytesFree.symbol(op.ctx)(&bytes)
	}
	return data, nil
}

func (op *Operator) readWithCancel(path string, token *opendalCancelToken, opts ...WithReadFn) (opendalBytes, error) {
	if len(opts) == 0 {
		return ffiOperatorReadWithCancel.symbol(op.ctx)(op.inner, path, token)
	}

	o := parseReadOptions(opts...)
	cOpts, keepAlive, err := newOpendalReadOptions(op.ctx, o)
	if err != nil {
		return opendalBytes{}, err
	}
	defer ffiReadOptionsFree.symbol(op.ctx)(cOpts)
	bytes, err := ffiOperatorReadWithOptionsCancel.symbol(op.ctx)(op.inner, path, cOpts, token)
	runtime.KeepAlive(keepAlive)
	return bytes, err
}

// WithReadFn is a functional option for read operations.
type WithReadFn func(*readOptions)

// ReadWithRange sets the byte range to read, starting at offset and reading
// length bytes. To read a file with size n, offset must be in [0, n) and the
// effective range is [offset, offset+length).
func ReadWithRange(offset, length uint64) WithReadFn {
	return func(o *readOptions) {
		o.hasRange = true
		o.rangeOffset = offset
		o.rangeLength = length
	}
}

// ReadWithVersion sets the version of the object to read.
func ReadWithVersion(version string) WithReadFn {
	return func(o *readOptions) {
		o.version = version
	}
}

// ReadWithIfMatch sets the If-Match condition for the read operation.
func ReadWithIfMatch(ifMatch string) WithReadFn {
	return func(o *readOptions) {
		o.ifMatch = ifMatch
	}
}

// ReadWithIfNoneMatch sets the If-None-Match condition for the read operation.
func ReadWithIfNoneMatch(ifNoneMatch string) WithReadFn {
	return func(o *readOptions) {
		o.ifNoneMatch = ifNoneMatch
	}
}

// ReadWithIfModifiedSince sets the If-Modified-Since condition for the read operation.
func ReadWithIfModifiedSince(t time.Time) WithReadFn {
	return func(o *readOptions) {
		millis := t.UnixMilli()
		o.ifModifiedSince = &millis
	}
}

// ReadWithIfUnmodifiedSince sets the If-Unmodified-Since condition for the read operation.
func ReadWithIfUnmodifiedSince(t time.Time) WithReadFn {
	return func(o *readOptions) {
		millis := t.UnixMilli()
		o.ifUnmodifiedSince = &millis
	}
}

// ReadWithConcurrent sets the number of concurrent read tasks.
func ReadWithConcurrent(concurrent uint) WithReadFn {
	return func(o *readOptions) {
		o.concurrent = concurrent
	}
}

// ReadWithChunk sets the chunk size for each read request.
func ReadWithChunk(chunk uint) WithReadFn {
	return func(o *readOptions) {
		o.chunk = chunk
	}
}

// ReadWithGap sets the gap size for merging nearby range reads.
func ReadWithGap(gap uint) WithReadFn {
	return func(o *readOptions) {
		o.gap = gap
	}
}

// ReadWithOverrideContentType sets the Content-Type to send back (presign only).
func ReadWithOverrideContentType(contentType string) WithReadFn {
	return func(o *readOptions) {
		o.overrideContentType = contentType
	}
}

// ReadWithOverrideCacheControl sets the Cache-Control to send back (presign only).
func ReadWithOverrideCacheControl(cacheControl string) WithReadFn {
	return func(o *readOptions) {
		o.overrideCacheControl = cacheControl
	}
}

// ReadWithOverrideContentDisposition sets the Content-Disposition to send back (presign only).
func ReadWithOverrideContentDisposition(contentDisposition string) WithReadFn {
	return func(o *readOptions) {
		o.overrideContentDisposition = contentDisposition
	}
}

type readOptions struct {
	hasRange                   bool
	rangeOffset                uint64
	rangeLength                uint64
	version                    string
	ifMatch                    string
	ifNoneMatch                string
	ifModifiedSince            *int64
	ifUnmodifiedSince          *int64
	concurrent                 uint
	chunk                      uint
	gap                        uint
	overrideContentType        string
	overrideCacheControl       string
	overrideContentDisposition string
}

func parseReadOptions(opts ...WithReadFn) *readOptions {
	o := &readOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

type readOptionsKeepAlive struct {
	strings [][]byte
}

func newOpendalReadOptions(ctx context.Context, o *readOptions) (*opendalReadOptions, readOptionsKeepAlive, error) {
	cOpts := ffiReadOptionsNew.symbol(ctx)()
	keepAlive := readOptionsKeepAlive{}

	// fail frees the C-allocated options before returning
	fail := func(err error) (*opendalReadOptions, readOptionsKeepAlive, error) {
		ffiReadOptionsFree.symbol(ctx)(cOpts)
		return nil, readOptionsKeepAlive{}, err
	}

	setString := func(value string, set func(*opendalReadOptions, string) ([]byte, error)) error {
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

	if o.hasRange {
		ffiReadOptionsSetRange.symbol(ctx)(cOpts, o.rangeOffset, o.rangeLength)
	}
	if err := setString(o.version, ffiReadOptionsSetVersion.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.ifMatch, ffiReadOptionsSetIfMatch.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.ifNoneMatch, ffiReadOptionsSetIfNoneMatch.symbol(ctx)); err != nil {
		return fail(err)
	}
	if o.ifModifiedSince != nil {
		ffiReadOptionsSetIfModifiedSince.symbol(ctx)(cOpts, *o.ifModifiedSince)
	}
	if o.ifUnmodifiedSince != nil {
		ffiReadOptionsSetIfUnmodifiedSince.symbol(ctx)(cOpts, *o.ifUnmodifiedSince)
	}
	if o.concurrent != 0 {
		ffiReadOptionsSetConcurrent.symbol(ctx)(cOpts, o.concurrent)
	}
	if o.chunk != 0 {
		ffiReadOptionsSetChunk.symbol(ctx)(cOpts, o.chunk)
	}
	if o.gap != 0 {
		ffiReadOptionsSetGap.symbol(ctx)(cOpts, o.gap)
	}
	if err := setString(o.overrideContentType, ffiReadOptionsSetOverrideContentType.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.overrideCacheControl, ffiReadOptionsSetOverrideCacheControl.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.overrideContentDisposition, ffiReadOptionsSetOverrideContentDisposition.symbol(ctx)); err != nil {
		return fail(err)
	}

	return cOpts, keepAlive, nil
}

// Reader creates a new Reader for reading the contents of a file at the specified path.
//
// This function is a wrapper around the C-binding function `opendal_operator_reader`.
//
// # Parameters
//
//   - ctx: The context bound to the returned Reader. It governs cancellation for
//     all subsequent Read and Seek calls on that Reader.
//   - path: The path of the file to read.
//
// # Returns
//
//   - *Reader: A reader for accessing the file's contents. It implements `io.ReadCloser`.
//   - error: An error if the reader creation fails, or nil if successful.
//
// # Notes
//
//   - This implementation does not support the `reader_with` functionality.
//   - The returned reader allows for more controlled and efficient reading of large files.
//   - The provided context is bound to the Reader; canceling it cancels in-flight
//     Read and Seek calls in a blocking manner.
//
// # Example
//
//	func exampleReader(op *opendal.Operator) {
//		r, err := op.Reader(context.Background(), "path/to/file")
//		if err != nil {
//			log.Fatal(err)
//		}
//		defer r.Close()
//
//		size := 1024 // Read 1KB at a time
//		buffer := make([]byte, size)
//
//		for {
//			n, err := r.Read(buffer)
//			if err != nil {
//				log.Fatal(err)
//			}
//			fmt.Printf("Read %d bytes: %s\n", n, buffer[:n])
//		}
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Reader(ctx context.Context, path string) (*Reader, error) {
	return runWithCancelContext(ctx, op.ctx, func(token *opendalCancelToken) (*Reader, error) {
		inner, err := ffiOperatorReaderWithCancel.symbol(op.ctx)(op.inner, path, token)
		if err != nil {
			return nil, err
		}
		reader := &Reader{
			inner:     inner,
			ctx:       op.ctx,
			cancelCtx: ctx,
		}
		return reader, nil
	}, func(reader *Reader) {
		if reader != nil {
			_ = reader.Close()
		}
	})
}

// Reader implements io.ReadSeekCloser.
//
// After a cancelled Read or Seek the handle remains valid and can be closed
// without leaking resources, but its internal stream position is unspecified.
// Callers should discard a Reader that had an operation cancelled and open a
// new one rather than attempting to resume reading from the same handle.
type Reader struct {
	inner *opendalReader
	ctx   context.Context
	// cancelCtx is the user-provided context bound at creation. It governs
	// cancellation for Read and Seek so the Reader keeps stdlib io interface
	// signatures.
	cancelCtx context.Context
}

var _ io.ReadSeekCloser = (*Reader)(nil)

// Read reads data from the underlying storage into the provided buffer.
//
// This method implements the io.Reader interface for OperatorReader.
//
// # Parameters
//
//   - buf: A pre-allocated byte slice where the read data will be stored.
//     The length of buf determines the maximum number of bytes to read.
//
// # Returns
//
//   - int: The number of bytes read. Returns 0 if no data is available or the end of the file is reached.
//   - error: An error if the read operation fails, or nil if successful.
//     Note that this method does not return io.EOF; it returns nil at the end of the file.
//
// # Notes
//
//   - The caller is responsible for pre-allocating the buffer and determining its size.
//
// # Example
//
//	reader, err := op.Reader(context.Background(), "path/to/file")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer reader.Close()
//
//	buf := make([]byte, 1024)
//	for {
//		n, err := reader.Read(buf)
//		if err != nil {
//			log.Fatal(err)
//		}
//		if n == 0 {
//			break // End of file
//		}
//		// Process buf[:n]
//	}
//
// Note: Always check the number of bytes read (n) as it may be less than len(buf).
//
// Read uses the context bound to the Reader at creation time. Canceling that
// context cancels the in-flight read in a blocking manner.
func (r *Reader) Read(buf []byte) (int, error) {
	return runWithCancelContext(r.cancelCtx, r.ctx, func(token *opendalCancelToken) (int, error) {
		length := uint(len(buf))
		if length == 0 {
			return 0, nil
		}
		read := ffiReaderReadWithCancel.symbol(r.ctx)
		var (
			totalSize uint
			size      uint
			err       error
		)
		for {
			size, err = read(r.inner, buf[totalSize:], token)
			totalSize += size
			if size == 0 || err != nil || totalSize >= length {
				break
			}
		}
		if totalSize == 0 && err == nil {
			err = io.EOF
		}
		return int(totalSize), err
	})
}

// Seek sets the offset for the next Read operation on the reader.
//
// This method implements the io.Seeker interface for Reader.
//
// # Parameters
//
//   - offset: The offset from the origin (specified by whence).
//   - whence: The reference point for offset. Can be:
//   - io.SeekStart (0): Relative to the start of the file
//   - io.SeekCurrent (1): Relative to the current position
//   - io.SeekEnd (2): Relative to the end of the file
//
// # Returns
//
//   - int64: The new absolute position in the file after the seek operation.
//   - error: An error if the seek operation fails, or nil if successful.
//
// # Example
//
//	reader, err := op.Reader(context.Background(), "path/to/file")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer reader.Close()
//
//	// Seek to the middle of the file
//	pos, err := reader.Seek(1000, io.SeekStart)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("New position: %d\n", pos)
//
//	// Seek relative to current position
//	pos, err = reader.Seek(100, io.SeekCurrent)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("New position: %d\n", pos)
//
// Note: The actual new position may differ from the requested position
// if the underlying storage system has restrictions on seeking.
//
// Seek uses the context bound to the Reader at creation time.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return runWithCancelContext(r.cancelCtx, r.ctx, func(token *opendalCancelToken) (int64, error) {
		return ffiReaderSeekWithCancel.symbol(r.ctx)(r.inner, offset, whence, token)
	})
}

// Close releases resources associated with the OperatorReader.
func (r *Reader) Close() error {
	ffiReaderFree.symbol(r.ctx)(r.inner)
	return nil
}

var ffiOperatorReadWithCancel = newFFI(ffiOpts{
	sym:    "opendal_operator_read_with_cancel",
	rType:  &typeResultRead,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string, token *opendalCancelToken) (opendalBytes, error) {
	return func(op *opendalOperator, path string, token *opendalCancelToken) (opendalBytes, error) {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return opendalBytes{}, err
		}
		var result resultRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&token),
		)
		return result.data, parseError(ctx, result.error)
	}
})

var ffiOperatorReadWithOptionsCancel = newFFI(ffiOpts{
	sym:    "opendal_operator_read_with_options_cancel",
	rType:  &typeResultRead,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string, opts *opendalReadOptions, token *opendalCancelToken) (opendalBytes, error) {
	return func(op *opendalOperator, path string, opts *opendalReadOptions, token *opendalCancelToken) (opendalBytes, error) {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return opendalBytes{}, err
		}
		var result resultRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&opts),
			unsafe.Pointer(&token),
		)
		return result.data, parseError(ctx, result.error)
	}
})

var ffiReadOptionsNew = newFFI(ffiOpts{
	sym:   "opendal_read_options_new",
	rType: &ffi.TypePointer,
}, func(_ context.Context, ffiCall ffiCall) func() *opendalReadOptions {
	return func() *opendalReadOptions {
		var opts *opendalReadOptions
		ffiCall(unsafe.Pointer(&opts))
		return opts
	}
})

var ffiReadOptionsFree = newFFI(ffiOpts{
	sym:    "opendal_read_options_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalReadOptions) {
	return func(opts *opendalReadOptions) {
		ffiCall(nil, unsafe.Pointer(&opts))
	}
})

var ffiReadOptionsSetRange = newFFI(ffiOpts{
	sym:    "opendal_read_options_set_range",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypeUint64, &ffi.TypeUint64},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalReadOptions, offset, length uint64) {
	return func(opts *opendalReadOptions, offset, length uint64) {
		ffiCall(nil, unsafe.Pointer(&opts), unsafe.Pointer(&offset), unsafe.Pointer(&length))
	}
})

func newReadOptionsSetStringFFI(sym string) *FFI[func(*opendalReadOptions, string) ([]byte, error)] {
	return newFFI(ffiOpts{
		sym:    contextKey(sym),
		rType:  &ffi.TypeVoid,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
	}, func(_ context.Context, ffiCall ffiCall) func(*opendalReadOptions, string) ([]byte, error) {
		return func(opts *opendalReadOptions, value string) ([]byte, error) {
			data, err := byteSliceFromString(value)
			if err != nil {
				return nil, err
			}
			byteValue := &data[0]
			ffiCall(nil, unsafe.Pointer(&opts), unsafe.Pointer(&byteValue))
			return data, nil
		}
	})
}

var ffiReadOptionsSetVersion = newReadOptionsSetStringFFI("opendal_read_options_set_version")
var ffiReadOptionsSetIfMatch = newReadOptionsSetStringFFI("opendal_read_options_set_if_match")
var ffiReadOptionsSetIfNoneMatch = newReadOptionsSetStringFFI("opendal_read_options_set_if_none_match")
var ffiReadOptionsSetOverrideContentType = newReadOptionsSetStringFFI("opendal_read_options_set_override_content_type")
var ffiReadOptionsSetOverrideCacheControl = newReadOptionsSetStringFFI("opendal_read_options_set_override_cache_control")
var ffiReadOptionsSetOverrideContentDisposition = newReadOptionsSetStringFFI("opendal_read_options_set_override_content_disposition")

var ffiReadOptionsSetIfModifiedSince = newFFI(ffiOpts{
	sym:    "opendal_read_options_set_if_modified_since",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypeSint64},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalReadOptions, millis int64) {
	return func(opts *opendalReadOptions, millis int64) {
		ffiCall(nil, unsafe.Pointer(&opts), unsafe.Pointer(&millis))
	}
})

var ffiReadOptionsSetIfUnmodifiedSince = newFFI(ffiOpts{
	sym:    "opendal_read_options_set_if_unmodified_since",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypeSint64},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalReadOptions, millis int64) {
	return func(opts *opendalReadOptions, millis int64) {
		ffiCall(nil, unsafe.Pointer(&opts), unsafe.Pointer(&millis))
	}
})

var ffiReadOptionsSetConcurrent = newFFI(ffiOpts{
	sym:    "opendal_read_options_set_concurrent",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalReadOptions, concurrent uint) {
	return func(opts *opendalReadOptions, concurrent uint) {
		ffiCall(nil, unsafe.Pointer(&opts), unsafe.Pointer(&concurrent))
	}
})

var ffiReadOptionsSetChunk = newFFI(ffiOpts{
	sym:    "opendal_read_options_set_chunk",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalReadOptions, chunk uint) {
	return func(opts *opendalReadOptions, chunk uint) {
		ffiCall(nil, unsafe.Pointer(&opts), unsafe.Pointer(&chunk))
	}
})

var ffiReadOptionsSetGap = newFFI(ffiOpts{
	sym:    "opendal_read_options_set_gap",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(opts *opendalReadOptions, gap uint) {
	return func(opts *opendalReadOptions, gap uint) {
		ffiCall(nil, unsafe.Pointer(&opts), unsafe.Pointer(&gap))
	}
})

var ffiOperatorReaderWithCancel = newFFI(ffiOpts{
	sym:    "opendal_operator_reader_with_cancel",
	rType:  &typeResultOperatorReader,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(op *opendalOperator, path string, token *opendalCancelToken) (*opendalReader, error) {
	return func(op *opendalOperator, path string, token *opendalCancelToken) (*opendalReader, error) {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultOperatorReader
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
			unsafe.Pointer(&token),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.reader, nil
	}
})

var ffiReaderFree = newFFI(ffiOpts{
	sym:    "opendal_reader_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(r *opendalReader) {
	return func(r *opendalReader) {
		ffiCall(
			nil,
			unsafe.Pointer(&r),
		)
	}
})

var ffiReaderReadWithCancel = newFFI(ffiOpts{
	sym:    "opendal_reader_read_with_cancel",
	rType:  &typeResultReaderRead,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(r *opendalReader, buf []byte, token *opendalCancelToken) (size uint, err error) {
	return func(r *opendalReader, buf []byte, token *opendalCancelToken) (size uint, err error) {
		var length = len(buf)
		if length == 0 {
			return 0, nil
		}
		bytePtr := &buf[0]
		var result resultReaderRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&r),
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&length),
			unsafe.Pointer(&token),
		)
		if result.error != nil {
			return 0, parseError(ctx, result.error)
		}
		return result.size, nil
	}
})

var ffiReaderSeekWithCancel = newFFI(ffiOpts{
	sym:    "opendal_reader_seek_with_cancel",
	rType:  &typeResultReaderSeek,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(r *opendalReader, offset int64, whence int, token *opendalCancelToken) (int64, error) {
	return func(r *opendalReader, offset int64, whence int, token *opendalCancelToken) (int64, error) {
		var result resultReaderSeek
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&r),
			unsafe.Pointer(&offset),
			unsafe.Pointer(&whence),
			unsafe.Pointer(&token),
		)
		if result.error != nil {
			return 0, parseError(ctx, result.error)
		}
		return int64(result.pos), nil
	}
})
