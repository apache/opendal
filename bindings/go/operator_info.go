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
	"unsafe"

	"github.com/jupiterrider/ffi"
	"golang.org/x/sys/unix"
)

// Info returns metadata about the Operator.
//
// This method provides access to essential information about the Operator,
// including its storage scheme, root path, name, and capabilities.
//
// Returns:
//   - *OperatorInfo: A pointer to an OperatorInfo struct containing the Operator's metadata.
func (op *Operator) Info() *OperatorInfo {
	newInfo := getFFI[operatorInfoNew](op.ctx, symOperatorInfoNew)
	inner := newInfo(op.inner)
	getFullCap := getFFI[operatorInfoGetFullCapability](op.ctx, symOperatorInfoGetFullCapability)
	getNativeCap := getFFI[operatorInfoGetNativeCapability](op.ctx, symOperatorInfoGetNativeCapability)
	getScheme := getFFI[operatorInfoGetScheme](op.ctx, symOperatorInfoGetScheme)
	getRoot := getFFI[operatorInfoGetRoot](op.ctx, symOperatorInfoGetRoot)
	getName := getFFI[operatorInfoGetName](op.ctx, symOperatorInfoGetName)

	free := getFFI[operatorInfoFree](op.ctx, symOperatorInfoFree)
	defer free(inner)

	return &OperatorInfo{
		scheme:    getScheme(inner),
		root:      getRoot(inner),
		name:      getName(inner),
		fullCap:   &Capability{inner: getFullCap(inner)},
		nativeCap: &Capability{inner: getNativeCap(inner)},
	}
}

// OperatorInfo provides metadata about an Operator instance.
//
// This struct contains essential information about the storage backend
// and its capabilities, allowing users to query details about the
// Operator they are working with.
type OperatorInfo struct {
	scheme    string
	root      string
	name      string
	fullCap   *Capability
	nativeCap *Capability
}

func (i *OperatorInfo) GetFullCapability() *Capability {
	return i.fullCap
}

func (i *OperatorInfo) GetNativeCapability() *Capability {
	return i.nativeCap
}

func (i *OperatorInfo) GetScheme() string {
	return i.scheme
}

func (i *OperatorInfo) GetRoot() string {
	return i.root
}

func (i *OperatorInfo) GetName() string {
	return i.name
}

// Capability represents the set of operations and features supported by an Operator.
//
// Each field indicates the support level for a specific capability:
//   - bool fields: false indicates no support, true indicates support.
//   - uint fields: Represent size limits or thresholds for certain operations.
//
// This struct covers a wide range of capabilities including:
//   - Basic operations: stat, read, write, delete, copy, rename, list
//   - Advanced features: multipart uploads, presigned URLs, batch operations
//   - Operation modifiers: cache control, content type, if-match conditions
//
// The capability information helps in understanding the functionalities
// available for a specific storage backend or Operator configuration.
type Capability struct {
	inner *opendalCapability
}

func (c *Capability) Stat() bool {
	return c.inner.stat == 1
}

func (c *Capability) StatWithIfmatch() bool {
	return c.inner.statWithIfmatch == 1
}

func (c *Capability) StatWithIfNoneMatch() bool {
	return c.inner.statWithIfNoneMatch == 1
}

func (c *Capability) Read() bool {
	return c.inner.read == 1
}

func (c *Capability) ReadWithIfmatch() bool {
	return c.inner.readWithIfmatch == 1
}

func (c *Capability) ReadWithIfMatchNone() bool {
	return c.inner.readWithIfMatchNone == 1
}

func (c *Capability) ReadWithOverrideCacheControl() bool {
	return c.inner.readWithOverrideCacheControl == 1
}

func (c *Capability) ReadWithOverrideContentDisposition() bool {
	return c.inner.readWithOverrideContentDisposition == 1
}

func (c *Capability) ReadWithOverrideContentType() bool {
	return c.inner.readWithOverrideContentType == 1
}

func (c *Capability) Write() bool {
	return c.inner.write == 1
}

func (c *Capability) WriteCanMulti() bool {
	return c.inner.writeCanMulti == 1
}

func (c *Capability) WriteCanEmpty() bool {
	return c.inner.writeCanEmpty == 1
}

func (c *Capability) WriteCanAppend() bool {
	return c.inner.writeCanAppend == 1
}

func (c *Capability) WriteWithContentType() bool {
	return c.inner.writeWithContentType == 1
}

func (c *Capability) WriteWithContentDisposition() bool {
	return c.inner.writeWithContentDisposition == 1
}

func (c *Capability) WriteWithCacheControl() bool {
	return c.inner.writeWithCacheControl == 1
}

func (c *Capability) WriteMultiMaxSize() uint {
	return c.inner.writeMultiMaxSize
}

func (c *Capability) WriteMultiMinSize() uint {
	return c.inner.writeMultiMinSize
}

func (c *Capability) WriteMultiAlignSize() uint {
	return c.inner.writeMultiAlignSize
}

func (c *Capability) WriteTotalMaxSize() uint {
	return c.inner.writeTotalMaxSize
}

func (c *Capability) CreateDir() bool {
	return c.inner.createDir == 1
}

func (c *Capability) Delete() bool {
	return c.inner.delete == 1
}

func (c *Capability) Copy() bool {
	return c.inner.copy == 1
}

func (c *Capability) Rename() bool {
	return c.inner.rename == 1
}

func (c *Capability) List() bool {
	return c.inner.list == 1
}

func (c *Capability) ListWithLimit() bool {
	return c.inner.listWithLimit == 1
}

func (c *Capability) ListWithStartAfter() bool {
	return c.inner.listWithStartAfter == 1
}

func (c *Capability) ListWithRecursive() bool {
	return c.inner.listWithRecursive == 1
}

func (c *Capability) Presign() bool {
	return c.inner.presign == 1
}

func (c *Capability) PresignRead() bool {
	return c.inner.presignRead == 1
}

func (c *Capability) PresignStat() bool {
	return c.inner.presignStat == 1
}

func (c *Capability) PresignWrite() bool {
	return c.inner.presignWrite == 1
}

func (c *Capability) Batch() bool {
	return c.inner.batch == 1
}

func (c *Capability) BatchDelete() bool {
	return c.inner.batchDelete == 1
}

func (c *Capability) BatchMaxOperations() uint {
	return c.inner.batchMaxOperations
}

func (c *Capability) Blocking() bool {
	return c.inner.blocking == 1
}

const symOperatorInfoNew = "opendal_operator_info_new"

type operatorInfoNew func(op *opendalOperator) *opendalOperatorInfo

var withOperatorInfoNew = withFFI(ffiOpts{
	sym:    symOperatorInfoNew,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoNew {
	return func(op *opendalOperator) *opendalOperatorInfo {
		var result *opendalOperatorInfo
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&op),
		)
		return result
	}
})

const symOperatorInfoFree = "opendal_operator_info_free"

type operatorInfoFree func(info *opendalOperatorInfo)

var withOperatorInfoFree = withFFI(ffiOpts{
	sym:    symOperatorInfoFree,
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoFree {
	return func(info *opendalOperatorInfo) {
		ffiCall(
			nil,
			unsafe.Pointer(&info),
		)
	}
})

const symOperatorInfoGetFullCapability = "opendal_operator_info_get_full_capability"

type operatorInfoGetFullCapability func(info *opendalOperatorInfo) *opendalCapability

var withOperatorInfoGetFullCapability = withFFI(ffiOpts{
	sym:    symOperatorInfoGetFullCapability,
	rType:  &typeCapability,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetFullCapability {
	return func(info *opendalOperatorInfo) *opendalCapability {
		var cap opendalCapability
		ffiCall(
			unsafe.Pointer(&cap),
			unsafe.Pointer(&info),
		)
		return &cap
	}
})

const symOperatorInfoGetNativeCapability = "opendal_operator_info_get_native_capability"

type operatorInfoGetNativeCapability func(info *opendalOperatorInfo) *opendalCapability

var withOperatorInfoGetNativeCapability = withFFI(ffiOpts{
	sym:    symOperatorInfoGetNativeCapability,
	rType:  &typeCapability,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetNativeCapability {
	return func(info *opendalOperatorInfo) *opendalCapability {
		var cap opendalCapability
		ffiCall(
			unsafe.Pointer(&cap),
			unsafe.Pointer(&info),
		)
		return &cap
	}
})

const symOperatorInfoGetScheme = "opendal_operator_info_get_scheme"

type operatorInfoGetScheme func(info *opendalOperatorInfo) string

var withOperatorInfoGetScheme = withFFI(ffiOpts{
	sym:    symOperatorInfoGetScheme,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetScheme {
	return func(info *opendalOperatorInfo) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&info),
		)
		return unix.BytePtrToString(bytePtr)
	}
})

const symOperatorInfoGetRoot = "opendal_operator_info_get_root"

type operatorInfoGetRoot func(info *opendalOperatorInfo) string

var withOperatorInfoGetRoot = withFFI(ffiOpts{
	sym:    symOperatorInfoGetRoot,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetRoot {
	return func(info *opendalOperatorInfo) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&info),
		)
		return unix.BytePtrToString(bytePtr)
	}
})

const symOperatorInfoGetName = "opendal_operator_info_get_name"

type operatorInfoGetName func(info *opendalOperatorInfo) string

var withOperatorInfoGetName = withFFI(ffiOpts{
	sym:    symOperatorInfoGetName,
	rType:  &ffi.TypePointer,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)) operatorInfoGetName {
	return func(info *opendalOperatorInfo) string {
		var bytePtr *byte
		ffiCall(
			unsafe.Pointer(&bytePtr),
			unsafe.Pointer(&info),
		)
		return unix.BytePtrToString(bytePtr)
	}
})
