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
	"testing"
	"unsafe"
)

func TestCopyCStringAndFreeNil(t *testing.T) {
	var freed int
	freeCString := func(*byte) {
		freed++
	}

	got := copyCStringAndFree(nil, freeCString)
	if got != "" {
		t.Fatalf("copyCStringAndFree(nil) = %q, want empty string", got)
	}
	if freed != 0 {
		t.Fatalf("copyCStringAndFree(nil) freed %d pointers, want 0", freed)
	}
}

func TestOperatorInfoCopiesAndFreesOwnedStrings(t *testing.T) {
	var freed []*byte
	freeCString := func(ptr *byte) {
		freed = append(freed, ptr)
	}

	schemePtr := mustBytePtrFromString(t, "memory")
	rootPtr := mustBytePtrFromString(t, "/tmp/")
	namePtr := mustBytePtrFromString(t, "namespace")
	infoInner := &opendalOperatorInfo{}
	fullCap := &opendalCapability{stat: 1}
	nativeCap := &opendalCapability{list: 1}
	infoFreed := 0

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiStringFree.opts.sym, freeCString)
	ctx = context.WithValue(ctx, ffiOperatorInfoNew.opts.sym, func(op *opendalOperator) *opendalOperatorInfo {
		if op == nil {
			t.Fatal("Info() passed nil operator")
		}
		return infoInner
	})
	ctx = context.WithValue(ctx, ffiOperatorInfoFree.opts.sym, func(info *opendalOperatorInfo) {
		if info != infoInner {
			t.Fatalf("Info() freed unexpected operator info: %p", info)
		}
		infoFreed++
	})
	ctx = context.WithValue(ctx, ffiOperatorInfoGetScheme.opts.sym, ffiOperatorInfoGetScheme.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertOperatorInfoPointer(t, infoInner, aValues...)
		*(**byte)(rValue) = schemePtr
	}))
	ctx = context.WithValue(ctx, ffiOperatorInfoGetRoot.opts.sym, ffiOperatorInfoGetRoot.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertOperatorInfoPointer(t, infoInner, aValues...)
		*(**byte)(rValue) = rootPtr
	}))
	ctx = context.WithValue(ctx, ffiOperatorInfoGetName.opts.sym, ffiOperatorInfoGetName.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertOperatorInfoPointer(t, infoInner, aValues...)
		*(**byte)(rValue) = namePtr
	}))
	ctx = context.WithValue(ctx, ffiOperatorInfoGetFullCapability.opts.sym, func(info *opendalOperatorInfo) *opendalCapability {
		if info != infoInner {
			t.Fatalf("Info() requested full capability for unexpected operator info: %p", info)
		}
		return fullCap
	})
	ctx = context.WithValue(ctx, ffiOperatorInfoGetNativeCapability.opts.sym, func(info *opendalOperatorInfo) *opendalCapability {
		if info != infoInner {
			t.Fatalf("Info() requested native capability for unexpected operator info: %p", info)
		}
		return nativeCap
	})

	op := &Operator{
		ctx:   ctx,
		inner: &opendalOperator{},
	}

	info := op.Info()
	if info.GetScheme() != "memory" {
		t.Fatalf("Info().GetScheme() = %q, want memory", info.GetScheme())
	}
	if info.GetRoot() != "/tmp/" {
		t.Fatalf("Info().GetRoot() = %q, want /tmp/", info.GetRoot())
	}
	if info.GetName() != "namespace" {
		t.Fatalf("Info().GetName() = %q, want namespace", info.GetName())
	}
	if !info.GetFullCapability().Stat() {
		t.Fatal("Info().GetFullCapability().Stat() = false, want true")
	}
	if !info.GetNativeCapability().List() {
		t.Fatal("Info().GetNativeCapability().List() = false, want true")
	}
	if infoFreed != 1 {
		t.Fatalf("Info() freed operator info %d times, want 1", infoFreed)
	}
	assertFreedPointers(t, freed, schemePtr, rootPtr, namePtr)
}

func TestNewEntryCopiesAndFreesOwnedStrings(t *testing.T) {
	var freed []*byte
	freeCString := func(ptr *byte) {
		freed = append(freed, ptr)
	}

	namePtr := mustBytePtrFromString(t, "file.txt")
	pathPtr := mustBytePtrFromString(t, "dir/file.txt")
	entryInner := &opendalEntry{}
	entryFreed := 0

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiStringFree.opts.sym, freeCString)
	ctx = context.WithValue(ctx, ffiEntryFree.opts.sym, func(entry *opendalEntry) {
		if entry != entryInner {
			t.Fatalf("newEntry() freed unexpected entry: %p", entry)
		}
		entryFreed++
	})
	ctx = context.WithValue(ctx, ffiEntryName.opts.sym, ffiEntryName.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertEntryPointer(t, entryInner, aValues...)
		*(**byte)(rValue) = namePtr
	}))
	ctx = context.WithValue(ctx, ffiEntryPath.opts.sym, ffiEntryPath.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertEntryPointer(t, entryInner, aValues...)
		*(**byte)(rValue) = pathPtr
	}))

	entry := newEntry(ctx, entryInner)
	if entry.Name() != "file.txt" {
		t.Fatalf("newEntry().Name() = %q, want file.txt", entry.Name())
	}
	if entry.Path() != "dir/file.txt" {
		t.Fatalf("newEntry().Path() = %q, want dir/file.txt", entry.Path())
	}
	if entryFreed != 1 {
		t.Fatalf("newEntry() freed entry %d times, want 1", entryFreed)
	}
	assertFreedPointers(t, freed, namePtr, pathPtr)
}

func mustBytePtrFromString(t *testing.T, value string) *byte {
	t.Helper()

	ptr, err := BytePtrFromString(value)
	if err != nil {
		t.Fatalf("BytePtrFromString(%q) failed: %v", value, err)
	}
	return ptr
}

func assertOperatorInfoPointer(t *testing.T, want *opendalOperatorInfo, aValues ...unsafe.Pointer) {
	t.Helper()

	if len(aValues) != 1 {
		t.Fatalf("operator info getter received %d arguments, want 1", len(aValues))
	}
	got := *(**opendalOperatorInfo)(aValues[0])
	if got != want {
		t.Fatalf("operator info getter received %p, want %p", got, want)
	}
}

func assertEntryPointer(t *testing.T, want *opendalEntry, aValues ...unsafe.Pointer) {
	t.Helper()

	if len(aValues) != 1 {
		t.Fatalf("entry getter received %d arguments, want 1", len(aValues))
	}
	got := *(**opendalEntry)(aValues[0])
	if got != want {
		t.Fatalf("entry getter received %p, want %p", got, want)
	}
}

func assertFreedPointers(t *testing.T, got []*byte, want ...*byte) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("freed %d pointers, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("freed pointer[%d] = %p, want %p", i, got[i], want[i])
		}
	}
}
