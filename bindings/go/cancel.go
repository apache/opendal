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

import "context"

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return newCanceledError(err)
	}
	return nil
}

// withCancelToken runs fn with a native cancel token wired to callCtx.
//
// For a nil or non-cancellable context (callCtx.Done() == nil), fn receives a
// nil token without allocating native resources. When callCtx is already done,
// fn is not called and a CodeCancelled error wrapping callCtx.Err() is returned.
//
// Otherwise, context.AfterFunc signals the native token when callCtx is done.
// Before freeing the token, the deferred cleanup stops the callback or waits
// for an in-progress callback to finish so opendal_cancel_token_cancel can
// never race with opendal_cancel_token_free.
func withCancelToken(ffiCtx, callCtx context.Context, fn func(token *opendalCancelToken) error) error {
	if callCtx == nil {
		return fn(nil)
	}
	if err := callCtx.Err(); err != nil {
		return newCanceledError(err)
	}
	if callCtx.Done() == nil {
		return fn(nil)
	}

	token := ffiCancelTokenNew.symbol(ffiCtx)()
	cancelFn := ffiCancelTokenCancel.symbol(ffiCtx)
	freeFn := ffiCancelTokenFree.symbol(ffiCtx)

	afterDone := make(chan struct{})
	stop := context.AfterFunc(callCtx, func() {
		defer close(afterDone)
		cancelFn(token)
	})
	defer func() {
		if !stop() {
			<-afterDone
		}
		freeFn(token)
	}()

	return wrapCanceledError(callCtx, fn(token))
}

func withCancelTokenValue[T any](ffiCtx, callCtx context.Context, fn func(token *opendalCancelToken) (T, error)) (T, error) {
	var res T
	err := withCancelToken(ffiCtx, callCtx, func(token *opendalCancelToken) error {
		var callErr error
		res, callErr = fn(token)
		return callErr
	})
	return res, err
}

var ffiCancelTokenNew = newDirectFFI[func() *opendalCancelToken]("opendal_cancel_token_new")

var ffiCancelTokenCancel = newDirectFFI[func(token *opendalCancelToken)]("opendal_cancel_token_cancel")

var ffiCancelTokenIsCancelled = newDirectFFI[func(token *opendalCancelToken) bool]("opendal_cancel_token_is_cancelled")

var ffiCancelTokenFree = newDirectFFI[func(token *opendalCancelToken)]("opendal_cancel_token_free")
