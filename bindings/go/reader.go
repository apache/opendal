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
	"io"
	"runtime"
	"time"
)

// Read reads the entire contents of the file at the specified path into a byte slice.
//
// Read is a wrapper around the C-binding function `opendal_operator_read`.
// When options are provided, it uses `opendal_operator_read_with`.
//
// # Parameters
//
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
//		data, err := op.Read("test")
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("Read: %s\n", data)
//	}
//
// Note: This example assumes proper error handling and import statements.
func (op *Operator) Read(path string, opts ...WithReadFn) ([]byte, error) {
	bytes, err := op.read(path, opts...)
	if err != nil {
		return nil, err
	}

	data := parseBytes(bytes)
	if bytes.data != nil || len(data) > 0 {
		ffiBytesFree.symbol(op.ctx)(&bytes)
	}
	return data, nil
}

func (op *Operator) read(path string, opts ...WithReadFn) (opendalBytes, error) {
	if len(opts) == 0 {
		return ffiOperatorRead.symbol(op.ctx)(op.inner, path)
	}

	o := parseReadOptions(opts...)
	cOpts, keepAlive, err := newOpendalReadOptions(op.ctx, o)
	if err != nil {
		return opendalBytes{}, err
	}
	defer ffiReadOptionsFree.symbol(op.ctx)(cOpts)
	bytes, err := ffiOperatorReadWith.symbol(op.ctx)(op.inner, path, cOpts)
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
		o.hasRangeLength = true
		o.rangeOffset = offset
		o.rangeLength = length
	}
}

// ReadWithRangeFrom sets the byte range to start at offset and read until the
// end of the file, i.e. the range [offset, n) for a file of size n.
func ReadWithRangeFrom(offset uint64) WithReadFn {
	return func(o *readOptions) {
		o.hasRange = true
		o.hasRangeLength = false
		o.rangeOffset = offset
		o.rangeLength = 0
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

// ReadWithContentLengthHint sets the known content length of the object.
//
// This is an execution hint that allows OpenDAL to avoid extra metadata
// requests while planning reads. It must not be used as an object identity
// or consistency condition.
func ReadWithContentLengthHint(length uint64) WithReadFn {
	return func(o *readOptions) {
		o.contentLengthHint = &length
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
	hasRangeLength             bool
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
	contentLengthHint          *uint64
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
		if o.hasRangeLength {
			ffiReadOptionsSetRange.symbol(ctx)(cOpts, o.rangeOffset, o.rangeLength)
		} else {
			ffiReadOptionsSetRangeFrom.symbol(ctx)(cOpts, o.rangeOffset)
		}
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
	if o.contentLengthHint != nil {
		ffiReadOptionsSetContentLengthHint.symbol(ctx)(cOpts, *o.contentLengthHint)
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

// WithReaderFn is a functional option for creating a Reader.
type WithReaderFn func(*readerOptions)

// ReaderWithVersion sets the version of the object to read.
func ReaderWithVersion(version string) WithReaderFn {
	return func(o *readerOptions) {
		o.version = version
	}
}

// ReaderWithIfMatch sets the If-Match condition for the reader.
func ReaderWithIfMatch(ifMatch string) WithReaderFn {
	return func(o *readerOptions) {
		o.ifMatch = ifMatch
	}
}

// ReaderWithIfNoneMatch sets the If-None-Match condition for the reader.
func ReaderWithIfNoneMatch(ifNoneMatch string) WithReaderFn {
	return func(o *readerOptions) {
		o.ifNoneMatch = ifNoneMatch
	}
}

// ReaderWithIfModifiedSince sets the If-Modified-Since condition for the reader.
func ReaderWithIfModifiedSince(t time.Time) WithReaderFn {
	return func(o *readerOptions) {
		millis := t.UnixMilli()
		o.ifModifiedSince = &millis
	}
}

// ReaderWithIfUnmodifiedSince sets the If-Unmodified-Since condition for the reader.
func ReaderWithIfUnmodifiedSince(t time.Time) WithReaderFn {
	return func(o *readerOptions) {
		millis := t.UnixMilli()
		o.ifUnmodifiedSince = &millis
	}
}

// ReaderWithContentLengthHint sets the known content length of the object.
//
// This is an execution hint that allows OpenDAL to avoid extra metadata
// requests while planning reads. It must not be used as an object identity
// or consistency condition.
func ReaderWithContentLengthHint(length uint64) WithReaderFn {
	return func(o *readerOptions) {
		o.contentLengthHint = &length
	}
}

// ReaderWithConcurrent sets the number of concurrent read tasks.
func ReaderWithConcurrent(concurrent uint) WithReaderFn {
	return func(o *readerOptions) {
		o.concurrent = concurrent
	}
}

// ReaderWithChunk sets the chunk size for each read request.
func ReaderWithChunk(chunk uint) WithReaderFn {
	return func(o *readerOptions) {
		o.chunk = chunk
	}
}

// ReaderWithGap sets the gap size for merging nearby range reads.
func ReaderWithGap(gap uint) WithReaderFn {
	return func(o *readerOptions) {
		o.gap = gap
	}
}

// ReaderWithPrefetch sets the number of prefetched byte ranges buffered during concurrent reads.
func ReaderWithPrefetch(prefetch uint) WithReaderFn {
	return func(o *readerOptions) {
		o.prefetch = prefetch
	}
}

type readerOptions struct {
	version           string
	ifMatch           string
	ifNoneMatch       string
	ifModifiedSince   *int64
	ifUnmodifiedSince *int64
	contentLengthHint *uint64
	concurrent        uint
	chunk             uint
	gap               uint
	prefetch          uint
}

func parseReaderOptions(opts ...WithReaderFn) *readerOptions {
	o := &readerOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func newOpendalReaderOptions(ctx context.Context, o *readerOptions) (*opendalReaderOptions, readOptionsKeepAlive, error) {
	cOpts := ffiReaderOptionsNew.symbol(ctx)()
	keepAlive := readOptionsKeepAlive{}

	// fail frees the C-allocated options before returning
	fail := func(err error) (*opendalReaderOptions, readOptionsKeepAlive, error) {
		ffiReaderOptionsFree.symbol(ctx)(cOpts)
		return nil, readOptionsKeepAlive{}, err
	}

	setString := func(value string, set func(*opendalReaderOptions, string) ([]byte, error)) error {
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

	if err := setString(o.version, ffiReaderOptionsSetVersion.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.ifMatch, ffiReaderOptionsSetIfMatch.symbol(ctx)); err != nil {
		return fail(err)
	}
	if err := setString(o.ifNoneMatch, ffiReaderOptionsSetIfNoneMatch.symbol(ctx)); err != nil {
		return fail(err)
	}
	if o.ifModifiedSince != nil {
		ffiReaderOptionsSetIfModifiedSince.symbol(ctx)(cOpts, *o.ifModifiedSince)
	}
	if o.ifUnmodifiedSince != nil {
		ffiReaderOptionsSetIfUnmodifiedSince.symbol(ctx)(cOpts, *o.ifUnmodifiedSince)
	}
	if o.contentLengthHint != nil {
		ffiReaderOptionsSetContentLengthHint.symbol(ctx)(cOpts, *o.contentLengthHint)
	}
	if o.concurrent != 0 {
		ffiReaderOptionsSetConcurrent.symbol(ctx)(cOpts, o.concurrent)
	}
	if o.chunk != 0 {
		ffiReaderOptionsSetChunk.symbol(ctx)(cOpts, o.chunk)
	}
	if o.gap != 0 {
		ffiReaderOptionsSetGap.symbol(ctx)(cOpts, o.gap)
	}
	if o.prefetch != 0 {
		ffiReaderOptionsSetPrefetch.symbol(ctx)(cOpts, o.prefetch)
	}

	return cOpts, keepAlive, nil
}

// Reader creates a new Reader for reading the contents of a file at the specified path.
//
// Reader is a wrapper around the C-binding function `opendal_operator_reader`.
// When options are provided, it uses `opendal_operator_reader_with`.
//
// # Parameters
//
//   - path: The path of the file to read.
//   - opts: Optional reader options such as conditional headers, version or
//     concurrency tuning.
//
// # Returns
//
//   - *Reader: A reader for accessing the file's contents. It implements `io.ReadCloser`.
//   - error: An error if the reader creation fails, or nil if successful.
//
// # Notes
//
//   - The returned reader allows for more controlled and efficient reading of large files.
//
// # Example
//
//	func exampleReader(op *opendal.Operator) {
//		r, err := op.Reader("path/to/file")
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
func (op *Operator) Reader(path string, opts ...WithReaderFn) (*Reader, error) {
	if len(opts) == 0 {
		inner, err := ffiOperatorReader.symbol(op.ctx)(op.inner, path)
		if err != nil {
			return nil, err
		}
		return &Reader{inner: inner, ctx: op.ctx}, nil
	}

	o := parseReaderOptions(opts...)
	cOpts, keepAlive, err := newOpendalReaderOptions(op.ctx, o)
	if err != nil {
		return nil, err
	}
	defer ffiReaderOptionsFree.symbol(op.ctx)(cOpts)
	inner, err := ffiOperatorReaderWith.symbol(op.ctx)(op.inner, path, cOpts)
	runtime.KeepAlive(keepAlive)
	if err != nil {
		return nil, err
	}
	return &Reader{inner: inner, ctx: op.ctx}, nil
}

type Reader struct {
	inner *opendalReader
	ctx   context.Context
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
//	reader, err := op.Reader("path/to/file")
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
func (r *Reader) Read(buf []byte) (int, error) {
	length := uint(len(buf))
	if length == 0 {
		return 0, nil
	}
	read := ffiReaderRead.symbol(r.ctx)
	var (
		totalSize uint
		size      uint
		err       error
	)
	for {
		size, err = read(r.inner, buf[totalSize:])
		totalSize += size
		if size == 0 || err != nil || totalSize >= length {
			break
		}
	}
	if totalSize == 0 && err == nil {
		err = io.EOF
	}
	return int(totalSize), err
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
//	reader, err := op.Reader("path/to/file")
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
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return ffiReaderSeek.symbol(r.ctx)(r.inner, offset, whence)
}

// Close releases resources associated with the OperatorReader.
func (r *Reader) Close() error {
	ffiReaderFree.symbol(r.ctx)(r.inner)
	return nil
}

var ffiOperatorRead = newFFI(
	"opendal_operator_read",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, data *opendalBytes) *opendalError) func(op *opendalOperator, path string) (opendalBytes, error) {
		return func(op *opendalOperator, path string) (opendalBytes, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return opendalBytes{}, err
			}
			var data opendalBytes
			e := cFunc(op, bytePath, &data)
			runtime.KeepAlive(bytePath)
			return data, parseError(ctx, e)
		}
	},
)

var ffiOperatorReadWith = newFFI(
	"opendal_operator_read_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, opts *opendalReadOptions, data *opendalBytes) *opendalError) func(op *opendalOperator, path string, opts *opendalReadOptions) (opendalBytes, error) {
		return func(op *opendalOperator, path string, opts *opendalReadOptions) (opendalBytes, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return opendalBytes{}, err
			}
			var data opendalBytes
			e := cFunc(op, bytePath, opts, &data)
			runtime.KeepAlive(bytePath)
			return data, parseError(ctx, e)
		}
	},
)

var ffiReadOptionsNew = newDirectFFI[func() *opendalReadOptions]("opendal_read_options_new")

var ffiReadOptionsFree = newDirectFFI[func(opts *opendalReadOptions)]("opendal_read_options_free")

var ffiReadOptionsSetRange = newDirectFFI[func(opts *opendalReadOptions, offset, length uint64)]("opendal_read_options_set_range")

var ffiReadOptionsSetRangeFrom = newDirectFFI[func(opts *opendalReadOptions, offset uint64)]("opendal_read_options_set_range_from")

func newReadOptionsSetStringFFI(sym string) *FFI[func(*opendalReadOptions, string) ([]byte, error), func(*opendalReadOptions, *byte)] {
	return newFFI(
		contextKey(sym),
		func(_ context.Context, cFunc func(opts *opendalReadOptions, value *byte)) func(*opendalReadOptions, string) ([]byte, error) {
			return func(opts *opendalReadOptions, value string) ([]byte, error) {
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

var ffiReadOptionsSetVersion = newReadOptionsSetStringFFI("opendal_read_options_set_version")
var ffiReadOptionsSetIfMatch = newReadOptionsSetStringFFI("opendal_read_options_set_if_match")
var ffiReadOptionsSetIfNoneMatch = newReadOptionsSetStringFFI("opendal_read_options_set_if_none_match")
var ffiReadOptionsSetOverrideContentType = newReadOptionsSetStringFFI("opendal_read_options_set_override_content_type")
var ffiReadOptionsSetOverrideCacheControl = newReadOptionsSetStringFFI("opendal_read_options_set_override_cache_control")
var ffiReadOptionsSetOverrideContentDisposition = newReadOptionsSetStringFFI("opendal_read_options_set_override_content_disposition")

var ffiReadOptionsSetIfModifiedSince = newDirectFFI[func(opts *opendalReadOptions, millis int64)]("opendal_read_options_set_if_modified_since")

var ffiReadOptionsSetIfUnmodifiedSince = newDirectFFI[func(opts *opendalReadOptions, millis int64)]("opendal_read_options_set_if_unmodified_since")

var ffiReadOptionsSetConcurrent = newDirectFFI[func(opts *opendalReadOptions, concurrent uint)]("opendal_read_options_set_concurrent")

var ffiReadOptionsSetChunk = newDirectFFI[func(opts *opendalReadOptions, chunk uint)]("opendal_read_options_set_chunk")

var ffiReadOptionsSetGap = newDirectFFI[func(opts *opendalReadOptions, gap uint)]("opendal_read_options_set_gap")

var ffiReadOptionsSetContentLengthHint = newDirectFFI[func(opts *opendalReadOptions, length uint64)]("opendal_read_options_set_content_length_hint")

var ffiOperatorReader = newFFI(
	"opendal_operator_reader",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, reader **opendalReader) *opendalError) func(op *opendalOperator, path string) (*opendalReader, error) {
		return func(op *opendalOperator, path string) (*opendalReader, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var reader *opendalReader
			e := cFunc(op, bytePath, &reader)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return reader, nil
		}
	},
)

var ffiOperatorReaderWith = newFFI(
	"opendal_operator_reader_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, opts *opendalReaderOptions, reader **opendalReader) *opendalError) func(op *opendalOperator, path string, opts *opendalReaderOptions) (*opendalReader, error) {
		return func(op *opendalOperator, path string, opts *opendalReaderOptions) (*opendalReader, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var reader *opendalReader
			e := cFunc(op, bytePath, opts, &reader)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return reader, nil
		}
	},
)

var ffiReaderOptionsNew = newDirectFFI[func() *opendalReaderOptions]("opendal_reader_options_new")

var ffiReaderOptionsFree = newDirectFFI[func(opts *opendalReaderOptions)]("opendal_reader_options_free")

func newReaderOptionsSetStringFFI(sym string) *FFI[func(*opendalReaderOptions, string) ([]byte, error), func(*opendalReaderOptions, *byte)] {
	return newFFI(
		contextKey(sym),
		func(_ context.Context, cFunc func(opts *opendalReaderOptions, value *byte)) func(*opendalReaderOptions, string) ([]byte, error) {
			return func(opts *opendalReaderOptions, value string) ([]byte, error) {
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

var ffiReaderOptionsSetVersion = newReaderOptionsSetStringFFI("opendal_reader_options_set_version")
var ffiReaderOptionsSetIfMatch = newReaderOptionsSetStringFFI("opendal_reader_options_set_if_match")
var ffiReaderOptionsSetIfNoneMatch = newReaderOptionsSetStringFFI("opendal_reader_options_set_if_none_match")

var ffiReaderOptionsSetIfModifiedSince = newDirectFFI[func(opts *opendalReaderOptions, millis int64)]("opendal_reader_options_set_if_modified_since")

var ffiReaderOptionsSetIfUnmodifiedSince = newDirectFFI[func(opts *opendalReaderOptions, millis int64)]("opendal_reader_options_set_if_unmodified_since")

var ffiReaderOptionsSetContentLengthHint = newDirectFFI[func(opts *opendalReaderOptions, length uint64)]("opendal_reader_options_set_content_length_hint")

var ffiReaderOptionsSetConcurrent = newDirectFFI[func(opts *opendalReaderOptions, concurrent uint)]("opendal_reader_options_set_concurrent")

var ffiReaderOptionsSetChunk = newDirectFFI[func(opts *opendalReaderOptions, chunk uint)]("opendal_reader_options_set_chunk")

var ffiReaderOptionsSetGap = newDirectFFI[func(opts *opendalReaderOptions, gap uint)]("opendal_reader_options_set_gap")

var ffiReaderOptionsSetPrefetch = newDirectFFI[func(opts *opendalReaderOptions, prefetch uint)]("opendal_reader_options_set_prefetch")

var ffiReaderFree = newDirectFFI[func(r *opendalReader)]("opendal_reader_free")

var ffiReaderRead = newFFI(
	"opendal_reader_read",
	func(ctx context.Context, cFunc func(r *opendalReader, buf *byte, length uint, size *uint) *opendalError) func(r *opendalReader, buf []byte) (size uint, err error) {
		return func(r *opendalReader, buf []byte) (size uint, err error) {
			length := uint(len(buf))
			if length == 0 {
				return 0, nil
			}
			var outSize uint
			e := cFunc(r, &buf[0], length, &outSize)
			runtime.KeepAlive(buf)
			if e != nil {
				return 0, parseError(ctx, e)
			}
			return outSize, nil
		}
	},
)

var ffiReaderSeek = newFFI(
	"opendal_reader_seek",
	func(ctx context.Context, cFunc func(r *opendalReader, offset int64, whence int, pos *uint64) *opendalError) func(r *opendalReader, offset int64, whence int) (int64, error) {
		return func(r *opendalReader, offset int64, whence int) (int64, error) {
			var pos uint64
			e := cFunc(r, offset, whence, &pos)
			if e != nil {
				return 0, parseError(ctx, e)
			}
			return int64(pos), nil
		}
	},
)
