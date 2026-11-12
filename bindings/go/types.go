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

import "unsafe"

type opendalCapability struct {
	stat                               uint8
	statWithIfMatch                    uint8
	statWithIfNoneMatch                uint8
	statWithIfModifiedSince            uint8
	statWithIfUnmodifiedSince          uint8
	statWithOverrideCacheControl       uint8
	statWithOverrideContentDisposition uint8
	statWithOverrideContentType        uint8
	statWithVersion                    uint8
	read                               uint8
	readWithIfMatch                    uint8
	readWithIfNoneMatch                uint8
	readWithOverrideCacheControl       uint8
	readWithOverrideContentDisposition uint8
	readWithOverrideContentType        uint8
	readWithIfModifiedSince            uint8
	readWithIfUnmodifiedSince          uint8
	readWithVersion                    uint8
	write                              uint8
	writeCanMulti                      uint8
	writeCanEmpty                      uint8
	writeCanAppend                     uint8
	writeWithContentType               uint8
	writeWithContentDisposition        uint8
	writeWithContentEncoding           uint8
	writeWithCacheControl              uint8
	writeWithIfMatch                   uint8
	writeWithIfNoneMatch               uint8
	writeWithIfNotExists               uint8
	writeWithUserMetadata              uint8
	writeMultiMaxSize                  uint
	writeMultiMinSize                  uint
	writeTotalMaxSize                  uint
	createDir                          uint8
	delete                             uint8
	deleteWithVersion                  uint8
	deleteWithRecursive                uint8
	copy                               uint8
	copyWithIfNotExists                uint8
	copyWithIfMatch                    uint8
	copyWithSourceVersion              uint8
	copyCanMulti                       uint8
	copyMultiMaxSize                   uint
	copyMultiMinSize                   uint
	rename                             uint8
	list                               uint8
	listWithLimit                      uint8
	listWithStartAfter                 uint8
	listWithRecursive                  uint8
	listWithVersions                   uint8
	listWithDeleted                    uint8
	presign                            uint8
	presignRead                        uint8
	presignStat                        uint8
	presignWrite                       uint8
	presignDelete                      uint8
	shared                             uint8
}

type opendalOperator struct{}

type opendalReader struct{}

type opendalWriter struct{}

type opendalCopier struct{}

type opendalPresignedRequest struct{}

type opendalHttpHeaderPair struct {
	key   *byte
	value *byte
}

type opendalMetadata struct{}

type opendalMetadataUserMetadata struct{}

type opendalMetadataUserMetadataPair struct {
	key   *byte
	value *byte
}

type opendalBytes struct {
	data     *byte
	len      uintptr
	capacity uintptr
}

type opendalError struct {
	code    int32
	message opendalBytes
}

type opendalOperatorInfo struct{}

type opendalLister struct{}

type opendalListOptions struct{}

type opendalDeleteOptions struct{}

type opendalReadOptions struct{}

type opendalReaderOptions struct{}

type opendalCopyOptions struct{}

type opendalWriteOptions struct{}

type opendalStatOptions struct{}

type opendalWriteUserMetadataPair struct {
	key   *byte
	value *byte
}

type opendalEntry struct{}

func toOpendalBytes(data []byte) *opendalBytes {
	l := len(data)
	if l == 0 {
		return &opendalBytes{}
	}

	return &opendalBytes{
		data:     &data[0],
		len:      uintptr(l),
		capacity: uintptr(cap(data)),
	}
}

func parseBytes(b opendalBytes) (data []byte) {
	if b.data == nil || b.len == 0 {
		return nil
	}
	data = make([]byte, b.len)
	copy(data, unsafe.Slice(b.data, b.len))
	return
}
