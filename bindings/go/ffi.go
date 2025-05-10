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
	"unsafe"

	"github.com/jupiterrider/ffi"
)

func contextWithFFIs(path string) (ctx context.Context, cancel context.CancelFunc, err error) {
	libopendal, err := LoadLibrary(path)
	if err != nil {
		return
	}
	ctx = context.Background()
	for _, withFFI := range withFFIs {
		ctx, err = withFFI(ctx, libopendal)
		if err != nil {
			return
		}
	}
	cancel = func() {
		_ = FreeLibrary(libopendal)
	}
	return
}

type contextWithFFI func(ctx context.Context, libopendal uintptr) (context.Context, error)

func getFFI[T any](ctx context.Context, key string) T {
	ctxKey := contextKey(key)
	return ctx.Value(ctxKey).(T)
}

type contextKey string

func (k contextKey) String() string {
	return string(k)
}

type ffiOpts struct {
	sym    contextKey
	rType  *ffi.Type
	aTypes []*ffi.Type
}

type ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)

func withFFI[T any](
	opts ffiOpts,
	withFunc func(
		ctx context.Context,
		ffiCall ffiCall,
	) T,
) func(ctx context.Context, libopendal uintptr) (context.Context, error) {
	return func(ctx context.Context, libopendal uintptr) (context.Context, error) {
		var cif ffi.Cif
		if status := ffi.PrepCif(
			&cif,
			ffi.DefaultAbi,
			uint32(len(opts.aTypes)),
			opts.rType,
			opts.aTypes...,
		); status != ffi.OK {
			return nil, errors.New(status.String())
		}
		fn, err := GetProcAddress(libopendal, opts.sym.String())
		if err != nil {
			return nil, err
		}
		return context.WithValue(ctx, opts.sym,
			withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
				ffi.Call(&cif, fn, rValue, aValues...)
			}),
		), nil
	}
}

var withFFIs = []contextWithFFI{
	// free must be on top
	withBytesFree,
	withErrorFree,

	withOperatorOptionsNew,
	withOperatorOptionsSet,
	withOperatorOptionsFree,

	withOperatorNew,
	withOperatorFree,

	withOperatorInfoNew,
	withOperatorInfoGetFullCapability,
	withOperatorInfoGetNativeCapability,
	withOperatorInfoGetScheme,
	withOperatorInfoGetRoot,
	withOperatorInfoGetName,
	withOperatorInfoFree,

	withOperatorCreateDir,
	withOperatorRead,
	withOperatorWrite,
	withOperatorDelete,
	withOperatorStat,
	withOperatorIsExists,
	withOperatorCopy,
	withOperatorRename,

	withMetaContentLength,
	withMetaIsFile,
	withMetaIsDir,
	withMetaLastModified,
	withMetaFree,

	withOperatorList,
	withListerNext,
	withListerFree,
	withEntryName,
	withEntryPath,
	withEntryFree,

	withOperatorReader,
	withReaderRead,
	withReaderSeek,
	withReaderFree,

	withOperatorWriter,
	withWriterWrite,
	withWriterFree,
	withWriterClose,
}
