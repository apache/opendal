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

type opendalOperator struct{}

type opendalCancelToken struct{}

type opendalReader struct{}

type opendalWriter struct{}

type opendalCopier struct{}

type opendalDeleter struct{}

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
