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
	"time"
	"unsafe"
)

// EntryMode indicates whether an entry is a file, directory, or unknown.
type EntryMode uint8

const (
	EntryModeUnknown EntryMode = iota
	EntryModeFile
	EntryModeDir
)

// Metadata represents essential information about a file or directory.
//
// This struct contains attributes commonly used in file systems
// and object storage systems.
type Metadata struct {
	cacheControl       *string
	contentDisposition *string
	contentEncoding    *string
	contentLength      uint64
	contentMD5         *string
	contentType        *string
	etag               *string
	isCurrent          *bool
	isDeleted          bool
	isFile             bool
	isDir              bool
	lastModified       time.Time
	mode               EntryMode
	version            *string
	userMetadata       map[string]string
}

func optionalStringValue(v *string) (string, bool) {
	if v == nil {
		return "", false
	}
	return *v, true
}

func boolPtrFromOptionalByte(v uint8) *bool {
	switch v {
	case 0:
		b := false
		return &b
	case 1:
		b := true
		return &b
	default:
		return nil
	}
}

func newMetadata(ctx context.Context, inner *opendalMetadata) *Metadata {

	var lastModified time.Time
	ms := ffiMetaLastModified.symbol(ctx)(inner)
	if ms != -1 {
		lastModified = time.UnixMilli(ms)
	}

	isCurrent := boolPtrFromOptionalByte(ffiMetaIsCurrent.symbol(ctx)(inner))

	defer ffiMetadataFree.symbol(ctx)(inner)

	return &Metadata{
		cacheControl:       ffiMetaCacheControl.symbol(ctx)(inner),
		contentDisposition: ffiMetaContentDisposition.symbol(ctx)(inner),
		contentEncoding:    ffiMetaContentEncoding.symbol(ctx)(inner),
		contentLength:      ffiMetaContentLength.symbol(ctx)(inner),
		contentMD5:         ffiMetaContentMD5.symbol(ctx)(inner),
		contentType:        ffiMetaContentType.symbol(ctx)(inner),
		etag:               ffiMetaEtag.symbol(ctx)(inner),
		isCurrent:          isCurrent,
		isDeleted:          ffiMetaIsDeleted.symbol(ctx)(inner),
		isFile:             ffiMetaIsFile.symbol(ctx)(inner),
		isDir:              ffiMetaIsDir.symbol(ctx)(inner),
		lastModified:       lastModified,
		mode:               ffiMetaMode.symbol(ctx)(inner),
		version:            ffiMetaVersion.symbol(ctx)(inner),
		userMetadata:       ffiMetaUserMetadata.symbol(ctx)(inner),
	}
}

// CacheControl returns the cache control of the entry.
func (m *Metadata) CacheControl() (string, bool) {
	return optionalStringValue(m.cacheControl)
}

// ContentDisposition returns the content disposition of the entry.
func (m *Metadata) ContentDisposition() (string, bool) {
	return optionalStringValue(m.contentDisposition)
}

// ContentEncoding returns the content encoding of the entry.
func (m *Metadata) ContentEncoding() (string, bool) {
	return optionalStringValue(m.contentEncoding)
}

// ContentMD5 returns the content MD5 of the entry.
func (m *Metadata) ContentMD5() (string, bool) {
	return optionalStringValue(m.contentMD5)
}

// ContentType returns the content type of the entry.
func (m *Metadata) ContentType() (string, bool) {
	return optionalStringValue(m.contentType)
}

// ETag returns the ETag of the entry.
func (m *Metadata) ETag() (string, bool) {
	return optionalStringValue(m.etag)
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

// IsCurrent returns whether this metadata represents the current version.
//
// The second return value is false when the service doesn't provide this information.
func (m *Metadata) IsCurrent() (bool, bool) {
	if m.isCurrent == nil {
		return false, false
	}
	return *m.isCurrent, true
}

// IsDeleted returns whether this metadata represents a deleted object or version.
func (m *Metadata) IsDeleted() bool {
	return m.isDeleted
}

// LastModified returns the time when the file or directory was last modified.
//
// The returned time is in UTC.
func (m *Metadata) LastModified() time.Time {
	return m.lastModified
}

// Mode returns the mode of the entry.
func (m *Metadata) Mode() EntryMode {
	return m.mode
}

// Version returns the version of the entry.
func (m *Metadata) Version() (string, bool) {
	return optionalStringValue(m.version)
}

// UserMetadata returns the user-defined metadata of the entry.
func (m *Metadata) UserMetadata() map[string]string {
	if m.userMetadata == nil {
		return nil
	}

	metadata := make(map[string]string, len(m.userMetadata))
	for key, value := range m.userMetadata {
		metadata[key] = value
	}
	return metadata
}

var ffiMetaMode = newFFI(
	"opendal_metadata_mode",
	func(_ context.Context, cFunc func(m *opendalMetadata) uint8) func(m *opendalMetadata) EntryMode {
		return func(m *opendalMetadata) EntryMode {
			return EntryMode(cFunc(m))
		}
	},
)

var ffiMetaContentLength = newDirectFFI[func(m *opendalMetadata) uint64]("opendal_metadata_content_length")

var ffiMetaCacheControl = newFFIMetadataString("opendal_metadata_cache_control")

var ffiMetaContentDisposition = newFFIMetadataString("opendal_metadata_content_disposition")

var ffiMetaContentMD5 = newFFIMetadataString("opendal_metadata_content_md5")

var ffiMetaContentType = newFFIMetadataString("opendal_metadata_content_type")

var ffiMetaContentEncoding = newFFIMetadataString("opendal_metadata_content_encoding")

var ffiMetaEtag = newFFIMetadataString("opendal_metadata_etag")

var ffiMetaVersion = newFFIMetadataString("opendal_metadata_version")

var ffiMetaIsFile = newDirectFFI[func(m *opendalMetadata) bool]("opendal_metadata_is_file")

var ffiMetaIsDir = newDirectFFI[func(m *opendalMetadata) bool]("opendal_metadata_is_dir")

var ffiMetaIsCurrent = newDirectFFI[func(m *opendalMetadata) uint8]("opendal_metadata_is_current")

var ffiMetaIsDeleted = newDirectFFI[func(m *opendalMetadata) bool]("opendal_metadata_is_deleted")

var ffiMetaLastModified = newDirectFFI[func(m *opendalMetadata) int64]("opendal_metadata_last_modified_ms")

var ffiMetaUserMetadata = newFFI(
	"opendal_metadata_get_user_metadata",
	func(ctx context.Context, cFunc func(m *opendalMetadata) *opendalMetadataUserMetadata) func(m *opendalMetadata) map[string]string {
		return func(m *opendalMetadata) map[string]string {
			metadata := cFunc(m)
			if metadata == nil {
				return nil
			}
			defer ffiMetaUserMetadataFree.symbol(ctx)(metadata)

			length := ffiMetaUserMetadataLen.symbol(ctx)(metadata)
			if length == 0 {
				return map[string]string{}
			}

			pairsPtr := ffiMetaUserMetadataPairs.symbol(ctx)(metadata)
			if pairsPtr == nil {
				return map[string]string{}
			}

			pairs := unsafe.Slice(pairsPtr, int(length))
			result := make(map[string]string, len(pairs))
			for _, pair := range pairs {
				key := BytePtrToString(pair.key)
				value := BytePtrToString(pair.value)
				result[key] = value
			}
			return result
		}
	},
)

var ffiMetaUserMetadataPairs = newDirectFFI[func(m *opendalMetadataUserMetadata) *opendalMetadataUserMetadataPair]("opendal_metadata_user_metadata_pairs")

var ffiMetaUserMetadataLen = newDirectFFI[func(m *opendalMetadataUserMetadata) uintptr]("opendal_metadata_user_metadata_len")

var ffiMetaUserMetadataFree = newDirectFFI[func(m *opendalMetadataUserMetadata)]("opendal_metadata_user_metadata_free")

func newFFIMetadataString(sym string) *FFI[func(m *opendalMetadata) *string, func(m *opendalMetadata) *byte] {
	_ = ffiStringFree
	return newFFI(
		contextKey(sym),
		func(ctx context.Context, cFunc func(m *opendalMetadata) *byte) func(m *opendalMetadata) *string {
			return func(m *opendalMetadata) *string {
				bytePtr := cFunc(m)
				if bytePtr == nil {
					return nil
				}
				value := copyCStringAndFree(bytePtr, ffiStringFree.symbol(ctx))
				return &value
			}
		},
	)
}

var ffiMetadataFree = newDirectFFI[func(m *opendalMetadata)]("opendal_metadata_free")
