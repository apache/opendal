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
	"fmt"
	"net/http"
	"runtime"
	"time"
	"unsafe"
)

type presignFunc func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error)

// PresignRead returns a presigned HTTP request that can be used to read the object at the given path.
func (op *Operator) PresignRead(path string, expire time.Duration, opts ...WithReadFn) (*http.Request, error) {
	if len(opts) == 0 {
		return op.presign(path, expire, ffiOperatorPresignRead.symbol(op.ctx))
	}

	o := parseReadOptions(opts...)
	cOpts, keepAlive, err := newOpendalReadOptions(op.ctx, o)
	if err != nil {
		return nil, err
	}
	defer ffiReadOptionsFree.symbol(op.ctx)(cOpts)
	req, err := ffiOperatorPresignReadWith.symbol(op.ctx)(op.inner, path, uint64(expire/time.Second), cOpts)
	runtime.KeepAlive(keepAlive)
	return op.buildPresignedRequest(req, err)
}

// PresignWrite returns a presigned HTTP request that can be used to write the object at the given path.
func (op *Operator) PresignWrite(path string, expire time.Duration, opts ...WithWriteFn) (*http.Request, error) {
	if len(opts) == 0 {
		return op.presign(path, expire, ffiOperatorPresignWrite.symbol(op.ctx))
	}

	o := parseWriteOptions(opts...)
	cOpts, keepAlive, err := newOpendalWriteOptions(op.ctx, o)
	if err != nil {
		return nil, err
	}
	defer ffiWriteOptionsFree.symbol(op.ctx)(cOpts)
	req, err := ffiOperatorPresignWriteWith.symbol(op.ctx)(op.inner, path, uint64(expire/time.Second), cOpts)
	runtime.KeepAlive(keepAlive)
	return op.buildPresignedRequest(req, err)
}

// PresignDelete returns a presigned HTTP request that can be used to delete the object at the given path.
func (op *Operator) PresignDelete(path string, expire time.Duration, opts ...WithDeleteFn) (*http.Request, error) {
	if len(opts) == 0 {
		return op.presign(path, expire, ffiOperatorPresignDelete.symbol(op.ctx))
	}

	o := parseDeleteOptions(opts...)
	cOpts, keepAlive, err := newOpendalDeleteOptions(op.ctx, o)
	if err != nil {
		return nil, err
	}
	defer ffiDeleteOptionsFree.symbol(op.ctx)(cOpts)
	req, err := ffiOperatorPresignDeleteWith.symbol(op.ctx)(op.inner, path, uint64(expire/time.Second), cOpts)
	runtime.KeepAlive(keepAlive)
	return op.buildPresignedRequest(req, err)
}

// PresignStat returns a presigned HTTP request that can be used to stat the object at the given path.
func (op *Operator) PresignStat(path string, expire time.Duration, opts ...WithStatFn) (*http.Request, error) {
	if len(opts) == 0 {
		return op.presign(path, expire, ffiOperatorPresignStat.symbol(op.ctx))
	}

	o := parseStatOptions(opts...)
	cOpts, keepAlive, err := newOpendalStatOptions(op.ctx, o)
	if err != nil {
		return nil, err
	}
	defer ffiStatOptionsFree.symbol(op.ctx)(cOpts)
	req, err := ffiOperatorPresignStatWith.symbol(op.ctx)(op.inner, path, uint64(expire/time.Second), cOpts)
	runtime.KeepAlive(keepAlive)
	return op.buildPresignedRequest(req, err)
}

func (op *Operator) presign(path string, expire time.Duration, call presignFunc) (*http.Request, error) {
	secs := uint64(expire / time.Second)

	req, err := call(op.inner, path, secs)
	return op.buildPresignedRequest(req, err)
}

func (op *Operator) buildPresignedRequest(req *opendalPresignedRequest, err error) (*http.Request, error) {
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, fmt.Errorf("presigned request should not be nil")
	}
	defer ffiPresignedRequestFree.symbol(op.ctx)(req)

	return buildHTTPPresignedRequest(op.ctx, req)
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

var ffiOperatorPresignRead = newFFI(
	"opendal_operator_presign_read",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, expire uint64, req **opendalPresignedRequest) *opendalError) func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
		return func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var req *opendalPresignedRequest
			e := cFunc(op, bytePath, expire, &req)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return req, nil
		}
	},
)

var ffiOperatorPresignReadWith = newFFI(
	"opendal_operator_presign_read_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, expire uint64, opts *opendalReadOptions, req **opendalPresignedRequest) *opendalError) func(op *opendalOperator, path string, expire uint64, opts *opendalReadOptions) (*opendalPresignedRequest, error) {
		return func(op *opendalOperator, path string, expire uint64, opts *opendalReadOptions) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var req *opendalPresignedRequest
			e := cFunc(op, bytePath, expire, opts, &req)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return req, nil
		}
	},
)

var ffiOperatorPresignWrite = newFFI(
	"opendal_operator_presign_write",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, expire uint64, req **opendalPresignedRequest) *opendalError) func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
		return func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var req *opendalPresignedRequest
			e := cFunc(op, bytePath, expire, &req)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return req, nil
		}
	},
)

var ffiOperatorPresignWriteWith = newFFI(
	"opendal_operator_presign_write_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, expire uint64, opts *opendalWriteOptions, req **opendalPresignedRequest) *opendalError) func(op *opendalOperator, path string, expire uint64, opts *opendalWriteOptions) (*opendalPresignedRequest, error) {
		return func(op *opendalOperator, path string, expire uint64, opts *opendalWriteOptions) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var req *opendalPresignedRequest
			e := cFunc(op, bytePath, expire, opts, &req)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return req, nil
		}
	},
)

var ffiOperatorPresignDelete = newFFI(
	"opendal_operator_presign_delete",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, expire uint64, req **opendalPresignedRequest) *opendalError) func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
		return func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var req *opendalPresignedRequest
			e := cFunc(op, bytePath, expire, &req)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return req, nil
		}
	},
)

var ffiOperatorPresignDeleteWith = newFFI(
	"opendal_operator_presign_delete_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, expire uint64, opts *opendalDeleteOptions, req **opendalPresignedRequest) *opendalError) func(op *opendalOperator, path string, expire uint64, opts *opendalDeleteOptions) (*opendalPresignedRequest, error) {
		return func(op *opendalOperator, path string, expire uint64, opts *opendalDeleteOptions) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var req *opendalPresignedRequest
			e := cFunc(op, bytePath, expire, opts, &req)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return req, nil
		}
	},
)

var ffiOperatorPresignStat = newFFI(
	"opendal_operator_presign_stat",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, expire uint64, req **opendalPresignedRequest) *opendalError) func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
		return func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var req *opendalPresignedRequest
			e := cFunc(op, bytePath, expire, &req)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return req, nil
		}
	},
)

var ffiOperatorPresignStatWith = newFFI(
	"opendal_operator_presign_stat_with",
	func(ctx context.Context, cFunc func(op *opendalOperator, path *byte, expire uint64, opts *opendalStatOptions, req **opendalPresignedRequest) *opendalError) func(op *opendalOperator, path string, expire uint64, opts *opendalStatOptions) (*opendalPresignedRequest, error) {
		return func(op *opendalOperator, path string, expire uint64, opts *opendalStatOptions) (*opendalPresignedRequest, error) {
			bytePath, err := BytePtrFromString(path)
			if err != nil {
				return nil, err
			}
			var req *opendalPresignedRequest
			e := cFunc(op, bytePath, expire, opts, &req)
			runtime.KeepAlive(bytePath)
			if e != nil {
				return nil, parseError(ctx, e)
			}
			return req, nil
		}
	},
)

var ffiPresignedRequestMethod = newDirectFFI[func(req *opendalPresignedRequest) *byte]("opendal_presigned_request_method")

var ffiPresignedRequestUri = newDirectFFI[func(req *opendalPresignedRequest) *byte]("opendal_presigned_request_uri")

var ffiPresignedRequestHeaders = newDirectFFI[func(req *opendalPresignedRequest) *opendalHttpHeaderPair]("opendal_presigned_request_headers")

var ffiPresignedRequestHeadersLen = newDirectFFI[func(req *opendalPresignedRequest) uintptr]("opendal_presigned_request_headers_len")

var ffiPresignedRequestFree = newDirectFFI[func(req *opendalPresignedRequest)]("opendal_presigned_request_free")
