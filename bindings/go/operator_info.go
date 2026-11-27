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

// Info returns metadata about the Operator.
//
// This method provides access to essential information about the Operator,
// including its storage scheme, root path, name, and capabilities.
//
// Returns:
//   - *OperatorInfo: A pointer to an OperatorInfo struct containing the
//     Operator's metadata, or nil once the Operator is closed.
func (op *Operator) Info() *OperatorInfo {
	opInner, release, err := op.acquire()
	if err != nil {
		return nil
	}
	defer release()
	inner := ffiOperatorInfoNew.symbol(op.ctx)(opInner)
	defer ffiOperatorInfoFree.symbol(op.ctx)(inner)

	return &OperatorInfo{
		scheme:     ffiOperatorInfoGetScheme.symbol(op.ctx)(inner),
		root:       ffiOperatorInfoGetRoot.symbol(op.ctx)(inner),
		name:       ffiOperatorInfoGetName.symbol(op.ctx)(inner),
		capability: &Capability{inner: ffiOperatorInfoGetCapability.symbol(op.ctx)(inner)},
	}
}

// OperatorInfo provides metadata about an Operator instance.
//
// This struct contains essential information about the storage backend
// and its capabilities, allowing users to query details about the
// Operator they are working with.
type OperatorInfo struct {
	scheme     string
	root       string
	name       string
	capability *Capability
}

func (i *OperatorInfo) GetCapability() *Capability {
	return i.capability
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

// StatWithIfmatch is a misspelled alias of StatWithIfMatch.
//
// Deprecated: use StatWithIfMatch instead.
func (c *Capability) StatWithIfmatch() bool {
	return c.StatWithIfMatch()
}

// Blocking always returns true.
//
// Deprecated: blocking is not a core capability; every operator supports it.
func (c *Capability) Blocking() bool {
	return true
}

var ffiOperatorInfoNew = newDirectFFI[func(op *opendalOperator) *opendalOperatorInfo]("opendal_operator_info_new")

var ffiOperatorInfoFree = newDirectFFI[func(info *opendalOperatorInfo)]("opendal_operator_info_free")

var ffiOperatorInfoGetCapability = newFFI(
	"opendal_operator_info_get_capability",
	func(_ context.Context, cFunc func(info *opendalOperatorInfo, cap *opendalCapability)) func(info *opendalOperatorInfo) *opendalCapability {
		return func(info *opendalOperatorInfo) *opendalCapability {
			var cap opendalCapability
			cFunc(info, &cap)
			return &cap
		}
	},
)

var ffiOperatorInfoGetScheme = func() *FFI[func(info *opendalOperatorInfo) string, func(info *opendalOperatorInfo) *byte] {
	_ = ffiStringFree
	return newFFI(
		"opendal_operator_info_get_scheme",
		func(ctx context.Context, cFunc func(info *opendalOperatorInfo) *byte) func(info *opendalOperatorInfo) string {
			return func(info *opendalOperatorInfo) string {
				return copyCStringAndFree(cFunc(info), ffiStringFree.symbol(ctx))
			}
		},
	)
}()

var ffiOperatorInfoGetRoot = func() *FFI[func(info *opendalOperatorInfo) string, func(info *opendalOperatorInfo) *byte] {
	_ = ffiStringFree
	return newFFI(
		"opendal_operator_info_get_root",
		func(ctx context.Context, cFunc func(info *opendalOperatorInfo) *byte) func(info *opendalOperatorInfo) string {
			return func(info *opendalOperatorInfo) string {
				return copyCStringAndFree(cFunc(info), ffiStringFree.symbol(ctx))
			}
		},
	)
}()

var ffiOperatorInfoGetName = func() *FFI[func(info *opendalOperatorInfo) string, func(info *opendalOperatorInfo) *byte] {
	_ = ffiStringFree
	return newFFI(
		"opendal_operator_info_get_name",
		func(ctx context.Context, cFunc func(info *opendalOperatorInfo) *byte) func(info *opendalOperatorInfo) string {
			return func(info *opendalOperatorInfo) string {
				return copyCStringAndFree(cFunc(info), ffiStringFree.symbol(ctx))
			}
		},
	)
}()
