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
	"time"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// Metadata represents essential information about a file or directory.
//
// This struct contains basic attributes commonly used in file systems
// and object storage systems.
type Metadata struct {
	contentLength uint64
	isFile        bool
	isDir         bool
	lastModified  time.Time
}

func newMetadata(ctx context.Context, inner *opendalMetadata) *Metadata {

	var lastModified time.Time
	ms := ffiMetaLastModified.symbol(ctx)(inner)
	if ms != -1 {
		lastModified = time.UnixMilli(ms)
	}

	defer ffiMetadataFree.symbol(ctx)(inner)

	return &Metadata{
		contentLength: ffiMetaContentLength.symbol(ctx)(inner),
		isFile:        ffiMetaIsFile.symbol(ctx)(inner),
		isDir:         ffiMetaIsDir.symbol(ctx)(inner),
		lastModified:  lastModified,
	}
}

// ContentLength returns the size of the file in bytes.
//
// For directories, this value may not be meaningful and could be zero.
func (m *Metadata) ContentLength() uint64 {
	return m.contentLength
}

// IsFile returns true if the metadata represents a file, false otherwise.
func (m *Metadata) IsFile() bool {
	return m.isFile
}

// IsDir returns true if the metadata represents a directory, false otherwise.
func (m *Metadata) IsDir() bool {
	return m.isDir
}

// LastModified returns the time when the file or directory was last modified.
//
// The returned time is in UTC.
func (m *Metadata) LastModified() time.Time {
	return m.lastModified
}

var ffiMetaContentLength = newFFI(ffiOpts{
	sym:    "opendal_metadata_content_length",
	rType:  &ffi.TypeUint64,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(m *opendalMetadata) uint64 {
	return func(m *opendalMetadata) uint64 {
		var length uint64
		ffiCall(
			unsafe.Pointer(&length),
			unsafe.Pointer(&m),
		)
		return length
	}
})

var ffiMetaIsFile = newFFI(ffiOpts{
	sym:    "opendal_metadata_is_file",
	rType:  &ffi.TypeUint8,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(m *opendalMetadata) bool {
	return func(m *opendalMetadata) bool {
		var result uint8
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result == 1
	}
})

var ffiMetaIsDir = newFFI(ffiOpts{
	sym:    "opendal_metadata_is_dir",
	rType:  &ffi.TypeUint8,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(m *opendalMetadata) bool {
	return func(m *opendalMetadata) bool {
		var result uint8
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result == 1
	}
})

var ffiMetaLastModified = newFFI(ffiOpts{
	sym:    "opendal_metadata_last_modified_ms",
	rType:  &ffi.TypeSint64,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(m *opendalMetadata) int64 {
	return func(m *opendalMetadata) int64 {
		var result int64
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&m),
		)
		return result
	}
})

var ffiMetadataFree = newFFI(ffiOpts{
	sym:    "opendal_metadata_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(m *opendalMetadata) {
	return func(m *opendalMetadata) {
		ffiCall(
			nil,
			unsafe.Pointer(&m),
		)
	}
})
