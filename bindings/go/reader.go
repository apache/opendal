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

// Read reads the entire contents of the file at the specified path into a byte slice.
//
// This function is a wrapper around the C-binding function `opendal_operator_read`.
//
// # Parameters
//
//   - path: The path of the file to read.
//
// # Returns
//
//   - []byte: The contents of the file as a byte slice.
//   - error: An error if the read operation fails, or nil if successful.
//
// # Notes
//
//   - This implementation does not support the `read_with` functionality.
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
func (op *Operator) Read(path string) ([]byte, error) {
	read := getFFI[operatorRead](op.ctx, symOperatorRead)
	bytes, err := read(op.inner, path)
	if err != nil {
		return nil, err
	}

	data := parseBytes(bytes)
	if len(data) > 0 {
		free := getFFI[bytesFree](op.ctx, symBytesFree)
		free(&bytes)
	}
	return data, nil
}

// Reader creates a new Reader for reading the contents of a file at the specified path.
//
// This function is a wrapper around the C-binding function `opendal_operator_reader`.
//
// # Parameters
//
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
func (op *Operator) Reader(path string) (*Reader, error) {
	getReader := getFFI[operatorReader](op.ctx, symOperatorReader)
	inner, err := getReader(op.inner, path)
	if err != nil {
		return nil, err
	}
	reader := &Reader{
		inner: inner,
		ctx:   op.ctx,
	}
	return reader, nil
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
	read := getFFI[readerRead](r.ctx, symReaderRead)
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
	seek := getFFI[readerSeek](r.ctx, symReaderSeek)
	return seek(r.inner, offset, whence)
}

// Close releases resources associated with the OperatorReader.
func (r *Reader) Close() error {
	free := getFFI[readerFree](r.ctx, symReaderFree)
	free(r.inner)
	return nil
}

const symOperatorRead = "opendal_operator_read"

type operatorRead func(op *opendalOperator, path string) (opendalBytes, error)

var withOperatorRead = withFFI(ffiOpts{
	sym:    symOperatorRead,
	rType:  &typeResultRead,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) operatorRead {
	return func(op *opendalOperator, path string) (opendalBytes, error) {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return opendalBytes{}, err
		}
		var result resultRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		return result.data, parseError(ctx, result.error)
	}
})

const symOperatorReader = "opendal_operator_reader"

type operatorReader func(op *opendalOperator, path string) (*opendalReader, error)

var withOperatorReader = withFFI(ffiOpts{
	sym:    symOperatorReader,
	rType:  &typeResultOperatorReader,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) operatorReader {
	return func(op *opendalOperator, path string) (*opendalReader, error) {
		bytePath, err := BytePtrFromString(path)
		if err != nil {
			return nil, err
		}
		var result resultOperatorReader
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
			unsafe.Pointer(&bytePath),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.reader, nil
	}
})

const symReaderFree = "opendal_reader_free"

type readerFree func(r *opendalReader)

var withReaderFree = withFFI(ffiOpts{
	sym:    symReaderFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) readerFree {
	return func(r *opendalReader) {
		ffiCall(
			nil,
			unsafe.Pointer(&r),
		)
	}
})

const symReaderRead = "opendal_reader_read"

type readerRead func(r *opendalReader, buf []byte) (size uint, err error)

var withReaderRead = withFFI(ffiOpts{
	sym:    symReaderRead,
	rType:  &typeResultReaderRead,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) readerRead {
	return func(r *opendalReader, buf []byte) (size uint, err error) {
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
		)
		if result.error != nil {
			return 0, parseError(ctx, result.error)
		}
		return result.size, nil
	}
})

const symReaderSeek = "opendal_reader_seek"

type readerSeek func(r *opendalReader, offset int64, whence int) (int64, error)

var withReaderSeek = withFFI(ffiOpts{
	sym:    symReaderSeek,
	rType:  &typeResultReaderSeek,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) readerSeek {
	return func(r *opendalReader, offset int64, whence int) (int64, error) {
		var result resultReaderSeek
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&r),
			unsafe.Pointer(&offset),
			unsafe.Pointer(&whence),
		)
		if result.error != nil {
			return 0, parseError(ctx, result.error)
		}
		return int64(result.pos), nil
	}
})
