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
	"fmt"
	"net/http"
	"time"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

type presignFunc func(op *opendalOperator, path string, expire uint64, token *opendalCancelToken) (*opendalPresignedRequest, error)

// PresignRead returns a presigned HTTP request that can be used to read the object at the given path.
//
// Canceling ctx cancels the underlying native call in a blocking manner.
func (op *Operator) PresignRead(ctx context.Context, path string, expire time.Duration) (*http.Request, error) {
	return op.presignContext(ctx, path, expire, func() presignFunc {
		return ffiOperatorPresignReadWithCancel.symbol(op.ctx)
	})
}

// PresignWrite returns a presigned HTTP request that can be used to write the object at the given path.
//
// Canceling ctx cancels the underlying native call in a blocking manner.
func (op *Operator) PresignWrite(ctx context.Context, path string, expire time.Duration) (*http.Request, error) {
	return op.presignContext(ctx, path, expire, func() presignFunc {
		return ffiOperatorPresignWriteWithCancel.symbol(op.ctx)
	})
}

// PresignDelete returns a presigned HTTP request that can be used to delete the object at the given path.
//
// Canceling ctx cancels the underlying native call in a blocking manner.
func (op *Operator) PresignDelete(ctx context.Context, path string, expire time.Duration) (*http.Request, error) {
	return op.presignContext(ctx, path, expire, func() presignFunc {
		return ffiOperatorPresignDeleteWithCancel.symbol(op.ctx)
	})
}

// PresignStat returns a presigned HTTP request that can be used to stat the object at the given path.
//
// Canceling ctx cancels the underlying native call in a blocking manner.
func (op *Operator) PresignStat(ctx context.Context, path string, expire time.Duration) (*http.Request, error) {
	return op.presignContext(ctx, path, expire, func() presignFunc {
		return ffiOperatorPresignStatWithCancel.symbol(op.ctx)
	})
}

func (op *Operator) presignContext(ctx context.Context, path string, expire time.Duration, call func() presignFunc) (*http.Request, error) {
	return runWithCancelContext(ctx, op.ctx, func(token *opendalCancelToken) (*http.Request, error) {
		secs := uint64(expire / time.Second)

		req, err := call()(op.inner, path, secs, token)
		if err != nil {
			return nil, err
		}
		if req == nil {
			return nil, fmt.Errorf("presigned request should not be nil")
		}
		defer ffiPresignedRequestFree.symbol(op.ctx)(req)

		return buildHTTPPresignedRequest(op.ctx, req)
	})
}

func buildHTTPPresignedRequest(ctx context.Context, ptr *opendalPresignedRequest) (req *http.Request, err error) {
	mptr := ffiPresignedRequestMethod.symbol(ctx)(ptr)
	uptr := ffiPresignedRequestUri.symbol(ctx)(ptr)

	method := BytePtrToString(mptr)
	uri := BytePtrToString(uptr)

	req, err = http.NewRequest(method, uri, nil)
	if err != nil {
		return
	}

	hptr := ffiPresignedRequestHeaders.symbol(ctx)(ptr)
	hl := ffiPresignedRequestHeadersLen.symbol(ctx)(ptr)
	if hptr == nil || hl == 0 {
		return
	}

	pairs := unsafe.Slice(hptr, int(hl))
	for i := range pairs {
		key := BytePtrToString(pairs[i].key)
		value := BytePtrToString(pairs[i].value)
		if key == "" {
			continue
		}
		req.Header.Add(key, value)
	}

	return
}

func newPresignWithCancelFFI(sym string) *FFI[presignFunc] {
	return newFFI(ffiOpts{
		sym:    contextKey(sym),
		rType:  &typeResultPresign,
		aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypeUint64, &ffi.TypePointer},
	}, func(ctx context.Context, ffiCall ffiCall) presignFunc {
		return func(op *opendalOperator, path string, expire uint64, token *opendalCancelToken) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var result resultPresign
			ffiCall(
				unsafe.Pointer(&result),
				unsafe.Pointer(&op),
				unsafe.Pointer(&bytePath),
				unsafe.Pointer(&expire),
				unsafe.Pointer(&token),
			)
			if result.error != nil {
				return nil, parseError(ctx, result.error)
			}
			return result.req, nil
		}
	})
}

var ffiOperatorPresignReadWithCancel = newPresignWithCancelFFI("opendal_operator_presign_read_with_cancel")
var ffiOperatorPresignWriteWithCancel = newPresignWithCancelFFI("opendal_operator_presign_write_with_cancel")
var ffiOperatorPresignDeleteWithCancel = newPresignWithCancelFFI("opendal_operator_presign_delete_with_cancel")
var ffiOperatorPresignStatWithCancel = newPresignWithCancelFFI("opendal_operator_presign_stat_with_cancel")

var ffiPresignedRequestMethod = newFFI(ffiOpts{
	sym:    "opendal_presigned_request_method",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(req *opendalPresignedRequest) *byte {
	return func(req *opendalPresignedRequest) *byte {
		var result *byte
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&req),
		)
		return result
	}
})

var ffiPresignedRequestUri = newFFI(ffiOpts{
	sym:    "opendal_presigned_request_uri",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(req *opendalPresignedRequest) *byte {
	return func(req *opendalPresignedRequest) *byte {
		var result *byte
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&req),
		)
		return result
	}
})

var ffiPresignedRequestHeaders = newFFI(ffiOpts{
	sym:    "opendal_presigned_request_headers",
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(req *opendalPresignedRequest) *opendalHttpHeaderPair {
	return func(req *opendalPresignedRequest) *opendalHttpHeaderPair {
		var result *opendalHttpHeaderPair
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&req),
		)
		return result
	}
})

var ffiPresignedRequestHeadersLen = newFFI(ffiOpts{
	sym:    "opendal_presigned_request_headers_len",
	rType:  &ffi.TypeUint64,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(req *opendalPresignedRequest) uintptr {
	return func(req *opendalPresignedRequest) uintptr {
		var length uint64
		ffiCall(
			unsafe.Pointer(&length),
			unsafe.Pointer(&req),
		)
		return uintptr(length)
	}
})

var ffiPresignedRequestFree = newFFI(ffiOpts{
	sym:    "opendal_presigned_request_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(req *opendalPresignedRequest) {
	return func(req *opendalPresignedRequest) {
		ffiCall(
			nil,
			unsafe.Pointer(&req),
		)
	}
})
