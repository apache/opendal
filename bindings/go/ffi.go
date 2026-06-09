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
	"errors"
	"sync"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

type ffiOpts struct {
	sym    contextKey
	rType  *ffi.Type
	aTypes []*ffi.Type
}

type ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)

type contextResult[T any] struct {
	value T
	err   error
}

// runWithCancelContext forwards Go context cancellation to native OpenDAL calls.
//
// When the context is canceled, the native cancel token is signaled and this
// helper blocks until the native call actually returns before reporting
// ctx.Err(). Waiting for the native call to finish guarantees that no native
// code is still touching caller-owned memory or operating on the underlying
// handle after this function returns, so callers can safely reuse buffers and
// close Reader/Writer/Lister handles once it returns.
//
// If the native call still produced a value after cancellation was requested
// (for example, it completed just before observing the token), that value is
// discarded through the optional cleanup callbacks since the caller only
// receives ctx.Err().
func runWithCancelContext[T any](ctx context.Context, ffiCtx context.Context, fn func(*opendalCancelToken) (T, error), cleanup ...func(T)) (T, error) {
	var zero T
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if ctx.Done() == nil {
		return fn(nil)
	}

	token := ffiCancelTokenNew.symbol(ffiCtx)()
	defer ffiCancelTokenFree.symbol(ffiCtx)(token)

	ch := make(chan contextResult[T], 1)
	go func() {
		value, err := fn(token)
		ch <- contextResult[T]{value: value, err: err}
	}()

	select {
	case result := <-ch:
		return result.value, result.err
	case <-ctx.Done():
		// Signal cancellation, then block until the native call returns so the
		// caller can safely reuse buffers and handles afterwards.
		ffiCancelTokenCancel.symbol(ffiCtx)(token)
		result := <-ch
		for _, cleanupFn := range cleanup {
			if cleanupFn != nil {
				cleanupFn(result.value)
			}
		}
		return zero, ctx.Err()
	}
}

func runErrWithCancelContext(ctx context.Context, ffiCtx context.Context, fn func(*opendalCancelToken) error) error {
	_, err := runWithCancelContext(ctx, ffiCtx, func(token *opendalCancelToken) (struct{}, error) {
		return struct{}{}, fn(token)
	})
	return err
}

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

type contextWithFFI func(ctx context.Context, lib uintptr) (context.Context, error)

type FFI[T any] struct {
	opts     ffiOpts
	withFunc func(ctx context.Context, ffiCall ffiCall) T
}

func newFFI[T any](opts ffiOpts, withFunc func(ctx context.Context, ffiCall ffiCall) T) *FFI[T] {
	ffi := &FFI[T]{
		opts:     opts,
		withFunc: withFunc,
	}
	withFFIs = append(withFFIs, ffi.withFFI)
	return ffi
}

func (f *FFI[T]) symbol(ctx context.Context) T {
	return ctx.Value(f.opts.sym).(T)
}

func (f *FFI[T]) withFFI(ctx context.Context, lib uintptr) (context.Context, error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif,
		ffi.DefaultAbi,
		uint32(len(f.opts.aTypes)),
		f.opts.rType,
		f.opts.aTypes...,
	); status != ffi.OK {
		return nil, errors.New(status.String())
	}
	fn, err := GetProcAddress(lib, f.opts.sym.String())
	if err != nil {
		return nil, err
	}
	val := f.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		ffi.Call(&cif, fn, rValue, aValues...)
	})
	return context.WithValue(ctx, f.opts.sym, val), nil
}

var withFFIs []contextWithFFI

var (
	libraryMu sync.Mutex
	libraries = map[string]uintptr{}
)

func loadSharedLibrary(path string) (uintptr, error) {
	libraryMu.Lock()
	defer libraryMu.Unlock()

	if lib, ok := libraries[path]; ok {
		return lib, nil
	}

	lib, err := LoadLibrary(path)
	if err != nil {
		return 0, err
	}
	libraries[path] = lib
	return lib, nil
}

func newContext(path string) (ctx context.Context, cancel context.CancelFunc, err error) {
	lib, err := loadSharedLibrary(path)
	if err != nil {
		return
	}
	ctx = context.Background()
	for _, withFFI := range withFFIs {
		ctx, err = withFFI(ctx, lib)
		if err != nil {
			return
		}
	}
	// Keep the library loaded for the process lifetime. The context stores libffi
	// call targets into this library, so unloading it during operator cleanup can
	// leave Go frames returning into unmapped code.
	cancel = func() {}

	return
}

var ffiCancelTokenNew = newFFI(ffiOpts{
	sym:   "opendal_cancel_token_new",
	rType: &ffi.TypePointer,
}, func(_ context.Context, ffiCall ffiCall) func() *opendalCancelToken {
	return func() *opendalCancelToken {
		var token *opendalCancelToken
		ffiCall(unsafe.Pointer(&token))
		return token
	}
})

var ffiCancelTokenCancel = newFFI(ffiOpts{
	sym:    "opendal_cancel_token_cancel",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(token *opendalCancelToken) {
	return func(token *opendalCancelToken) {
		ffiCall(
			nil,
			unsafe.Pointer(&token),
		)
	}
})

var ffiCancelTokenFree = newFFI(ffiOpts{
	sym:    "opendal_cancel_token_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(token *opendalCancelToken) {
	return func(token *opendalCancelToken) {
		ffiCall(
			nil,
			unsafe.Pointer(&token),
		)
	}
})
