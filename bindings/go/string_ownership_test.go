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
	"time"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

func TestEntryFreeUsesVoidReturnType(t *testing.T) {
	if ffiEntryFree.opts.rType != &ffi.TypeVoid {
		t.Fatalf("ffiEntryFree return type = %v, want void", ffiEntryFree.opts.rType)
	}
}

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

	// Mock the new metadata path exercised by newEntry()
	metaInner := &opendalMetadata{}
	metaFreed := 0
	ctx = context.WithValue(ctx, ffiEntryMetadata.opts.sym, func(e *opendalEntry) *opendalMetadata {
		if e != entryInner {
			t.Fatalf("ffiEntryMetadata called with unexpected entry")
		}
		return metaInner
	})
	ctx = context.WithValue(ctx, ffiMetadataFree.opts.sym, func(m *opendalMetadata) {
		if m != metaInner {
			t.Fatalf("metadata freed unexpected pointer: %p", m)
		}
		metaFreed++
	})
	ctx = context.WithValue(ctx, ffiMetaCacheControl.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentDisposition.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentEncoding.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentLength.opts.sym, func(m *opendalMetadata) uint64 {
		assertMetadataPointer(t, metaInner, m)
		return 4096
	})
	ctx = context.WithValue(ctx, ffiMetaContentMD5.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	contentType := "text/plain"
	ctx = context.WithValue(ctx, ffiMetaContentType.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return &contentType
	})
	ctx = context.WithValue(ctx, ffiMetaEtag.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaIsCurrent.opts.sym, func(m *opendalMetadata) uint8 {
		assertMetadataPointer(t, metaInner, m)
		return 2
	})
	ctx = context.WithValue(ctx, ffiMetaIsDeleted.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metaInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsFile.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metaInner, m)
		return true
	})
	ctx = context.WithValue(ctx, ffiMetaIsDir.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metaInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaLastModified.opts.sym, func(m *opendalMetadata) int64 {
		assertMetadataPointer(t, metaInner, m)
		return 1700000000000
	})
	ctx = context.WithValue(ctx, ffiMetaMode.opts.sym, func(m *opendalMetadata) EntryMode {
		assertMetadataPointer(t, metaInner, m)
		return EntryModeFile
	})
	ctx = context.WithValue(ctx, ffiMetaVersion.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadata.opts.sym, func(m *opendalMetadata) map[string]string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})

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

	// Verify new Metadata() support
	meta := entry.Metadata()
	if meta == nil {
		t.Fatal("newEntry().Metadata() returned nil, expected non-nil")
	}
	if meta.ContentLength() != 4096 {
		t.Fatalf("Metadata().ContentLength() = %d, want 4096", meta.ContentLength())
	}
	if !meta.IsFile() {
		t.Fatal("Metadata().IsFile() = false, want true")
	}
	if meta.IsDir() {
		t.Fatal("Metadata().IsDir() = true, want false")
	}
	if meta.LastModified().UnixMilli() != 1700000000000 {
		t.Fatalf("Metadata().LastModified().UnixMilli() = %d, want 1700000000000", meta.LastModified().UnixMilli())
	}
	if meta.Mode() != EntryModeFile {
		t.Fatalf("Metadata().Mode() = %d, want %d", meta.Mode(), EntryModeFile)
	}
	if contentType, ok := meta.ContentType(); !ok || contentType != "text/plain" {
		t.Fatalf("Metadata().ContentType() = %q, %v, want text/plain, true", contentType, ok)
	}
	if _, ok := meta.CacheControl(); ok {
		t.Fatal("Metadata().CacheControl() ok = true, want false")
	}
	if metaFreed != 1 {
		t.Fatalf("newEntry() freed metadata %d times, want 1", metaFreed)
	}
}

func TestNewMetadataCopiesAllFieldsAndFreesOwnedValues(t *testing.T) {
	var freed []*byte
	freeCString := func(ptr *byte) {
		freed = append(freed, ptr)
	}

	cacheControlPtr := mustBytePtrFromString(t, "max-age=60")
	contentDispositionPtr := mustBytePtrFromString(t, "attachment")
	contentEncodingPtr := mustBytePtrFromString(t, "gzip")
	contentMD5Ptr := mustBytePtrFromString(t, "1B2M2Y8AsgTpgAmY7PhCfg==")
	contentTypePtr := mustBytePtrFromString(t, "application/json")
	etagPtr := mustBytePtrFromString(t, `"etag"`)
	versionPtr := mustBytePtrFromString(t, "v42")

	metadataInner := &opendalMetadata{}
	userMetadataInner := &opendalMetadataUserMetadata{}
	pairs := []opendalMetadataUserMetadataPair{
		{key: mustBytePtrFromString(t, "foo"), value: mustBytePtrFromString(t, "bar")},
		{key: mustBytePtrFromString(t, "empty"), value: mustBytePtrFromString(t, "")},
	}
	metadataFreed := 0
	userMetadataFreed := 0

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiStringFree.opts.sym, freeCString)
	ctx = context.WithValue(ctx, ffiMetadataFree.opts.sym, func(m *opendalMetadata) {
		assertMetadataPointer(t, metadataInner, m)
		metadataFreed++
	})
	ctx = context.WithValue(ctx, ffiMetaCacheControl.opts.sym, ffiMetaCacheControl.withFunc(ctx, metadataStringFunc(t, metadataInner, cacheControlPtr)))
	ctx = context.WithValue(ctx, ffiMetaContentDisposition.opts.sym, ffiMetaContentDisposition.withFunc(ctx, metadataStringFunc(t, metadataInner, contentDispositionPtr)))
	ctx = context.WithValue(ctx, ffiMetaContentEncoding.opts.sym, ffiMetaContentEncoding.withFunc(ctx, metadataStringFunc(t, metadataInner, contentEncodingPtr)))
	ctx = context.WithValue(ctx, ffiMetaContentLength.opts.sym, func(m *opendalMetadata) uint64 {
		assertMetadataPointer(t, metadataInner, m)
		return 4096
	})
	ctx = context.WithValue(ctx, ffiMetaContentMD5.opts.sym, ffiMetaContentMD5.withFunc(ctx, metadataStringFunc(t, metadataInner, contentMD5Ptr)))
	ctx = context.WithValue(ctx, ffiMetaContentType.opts.sym, ffiMetaContentType.withFunc(ctx, metadataStringFunc(t, metadataInner, contentTypePtr)))
	ctx = context.WithValue(ctx, ffiMetaEtag.opts.sym, ffiMetaEtag.withFunc(ctx, metadataStringFunc(t, metadataInner, etagPtr)))
	ctx = context.WithValue(ctx, ffiMetaIsCurrent.opts.sym, func(m *opendalMetadata) uint8 {
		assertMetadataPointer(t, metadataInner, m)
		return 1
	})
	ctx = context.WithValue(ctx, ffiMetaIsDeleted.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return true
	})
	ctx = context.WithValue(ctx, ffiMetaIsFile.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsDir.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return true
	})
	ctx = context.WithValue(ctx, ffiMetaLastModified.opts.sym, func(m *opendalMetadata) int64 {
		assertMetadataPointer(t, metadataInner, m)
		return 1700000000000
	})
	ctx = context.WithValue(ctx, ffiMetaMode.opts.sym, func(m *opendalMetadata) EntryMode {
		assertMetadataPointer(t, metadataInner, m)
		return EntryModeDir
	})
	ctx = context.WithValue(ctx, ffiMetaVersion.opts.sym, ffiMetaVersion.withFunc(ctx, metadataStringFunc(t, metadataInner, versionPtr)))
	ctx = context.WithValue(ctx, ffiMetaUserMetadataPairs.opts.sym, ffiMetaUserMetadataPairs.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertUserMetadataPointerFromArgs(t, userMetadataInner, aValues...)
		*(**opendalMetadataUserMetadataPair)(rValue) = &pairs[0]
	}))
	ctx = context.WithValue(ctx, ffiMetaUserMetadataLen.opts.sym, ffiMetaUserMetadataLen.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertUserMetadataPointerFromArgs(t, userMetadataInner, aValues...)
		*(*uint64)(rValue) = uint64(len(pairs))
	}))
	ctx = context.WithValue(ctx, ffiMetaUserMetadataFree.opts.sym, func(m *opendalMetadataUserMetadata) {
		if m != userMetadataInner {
			t.Fatalf("user metadata freed unexpected pointer: %p", m)
		}
		userMetadataFreed++
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadata.opts.sym, ffiMetaUserMetadata.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertMetadataPointerFromArgs(t, metadataInner, aValues...)
		*(**opendalMetadataUserMetadata)(rValue) = userMetadataInner
	}))

	meta := newMetadata(ctx, metadataInner)
	assertOptionalString(t, "CacheControl", meta.CacheControl, "max-age=60")
	assertOptionalString(t, "ContentDisposition", meta.ContentDisposition, "attachment")
	assertOptionalString(t, "ContentEncoding", meta.ContentEncoding, "gzip")
	assertOptionalString(t, "ContentMD5", meta.ContentMD5, "1B2M2Y8AsgTpgAmY7PhCfg==")
	assertOptionalString(t, "ContentType", meta.ContentType, "application/json")
	assertOptionalString(t, "ETag", meta.ETag, `"etag"`)
	assertOptionalString(t, "Version", meta.Version, "v42")
	if meta.ContentLength() != 4096 {
		t.Fatalf("ContentLength() = %d, want 4096", meta.ContentLength())
	}
	current, ok := meta.IsCurrent()
	if !ok || !current {
		t.Fatalf("IsCurrent() = %v, %v, want true, true", current, ok)
	}
	if !meta.IsDeleted() {
		t.Fatal("IsDeleted() = false, want true")
	}
	if meta.IsFile() {
		t.Fatal("IsFile() = true, want false")
	}
	if !meta.IsDir() {
		t.Fatal("IsDir() = false, want true")
	}
	if meta.LastModified().UnixMilli() != 1700000000000 {
		t.Fatalf("LastModified().UnixMilli() = %d, want 1700000000000", meta.LastModified().UnixMilli())
	}
	if meta.Mode() != EntryModeDir {
		t.Fatalf("Mode() = %d, want %d", meta.Mode(), EntryModeDir)
	}
	assertStringMap(t, meta.UserMetadata(), map[string]string{"foo": "bar", "empty": ""})

	userMetadata := meta.UserMetadata()
	userMetadata["foo"] = "changed"
	assertStringMap(t, meta.UserMetadata(), map[string]string{"foo": "bar", "empty": ""})

	if metadataFreed != 1 {
		t.Fatalf("metadata freed %d times, want 1", metadataFreed)
	}
	if userMetadataFreed != 1 {
		t.Fatalf("user metadata freed %d times, want 1", userMetadataFreed)
	}
	assertFreedPointers(t, freed, cacheControlPtr, contentDispositionPtr, contentEncodingPtr, contentMD5Ptr, contentTypePtr, etagPtr, versionPtr)
}

func TestNewMetadataOptionalValuesCanBeAbsent(t *testing.T) {
	metadataInner := &opendalMetadata{}
	metadataFreed := 0

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiMetadataFree.opts.sym, func(m *opendalMetadata) {
		assertMetadataPointer(t, metadataInner, m)
		metadataFreed++
	})
	ctx = context.WithValue(ctx, ffiMetaCacheControl.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentDisposition.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentEncoding.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentLength.opts.sym, func(m *opendalMetadata) uint64 {
		assertMetadataPointer(t, metadataInner, m)
		return 0
	})
	ctx = context.WithValue(ctx, ffiMetaContentMD5.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentType.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaEtag.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaIsCurrent.opts.sym, func(m *opendalMetadata) uint8 {
		assertMetadataPointer(t, metadataInner, m)
		return 2
	})
	ctx = context.WithValue(ctx, ffiMetaIsDeleted.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsFile.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsDir.opts.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaLastModified.opts.sym, func(m *opendalMetadata) int64 {
		assertMetadataPointer(t, metadataInner, m)
		return -1
	})
	ctx = context.WithValue(ctx, ffiMetaMode.opts.sym, func(m *opendalMetadata) EntryMode {
		assertMetadataPointer(t, metadataInner, m)
		return EntryModeUnknown
	})
	ctx = context.WithValue(ctx, ffiMetaVersion.opts.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadata.opts.sym, func(m *opendalMetadata) map[string]string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})

	meta := newMetadata(ctx, metadataInner)
	if _, ok := meta.CacheControl(); ok {
		t.Fatal("CacheControl() ok = true, want false")
	}
	if _, ok := meta.ContentDisposition(); ok {
		t.Fatal("ContentDisposition() ok = true, want false")
	}
	if _, ok := meta.ContentEncoding(); ok {
		t.Fatal("ContentEncoding() ok = true, want false")
	}
	if _, ok := meta.ContentMD5(); ok {
		t.Fatal("ContentMD5() ok = true, want false")
	}
	if _, ok := meta.ContentType(); ok {
		t.Fatal("ContentType() ok = true, want false")
	}
	if _, ok := meta.ETag(); ok {
		t.Fatal("ETag() ok = true, want false")
	}
	if _, ok := meta.Version(); ok {
		t.Fatal("Version() ok = true, want false")
	}
	if current, ok := meta.IsCurrent(); ok || current {
		t.Fatalf("IsCurrent() = %v, %v, want false, false", current, ok)
	}
	if !meta.LastModified().IsZero() {
		t.Fatalf("LastModified() = %v, want zero", meta.LastModified())
	}
	if meta.UserMetadata() != nil {
		t.Fatalf("UserMetadata() = %v, want nil", meta.UserMetadata())
	}
	if metadataFreed != 1 {
		t.Fatalf("metadata freed %d times, want 1", metadataFreed)
	}
}

func TestBoolPtrFromOptionalByte(t *testing.T) {
	if got := boolPtrFromOptionalByte(2); got != nil {
		t.Fatalf("boolPtrFromOptionalByte(2) = %v, want nil", *got)
	}
	for _, tc := range []struct {
		value uint8
		want  bool
	}{
		{value: 0, want: false},
		{value: 1, want: true},
	} {
		got := boolPtrFromOptionalByte(tc.value)
		if got == nil || *got != tc.want {
			t.Fatalf("boolPtrFromOptionalByte(%d) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func mustBytePtrFromString(t *testing.T, value string) *byte {
	t.Helper()

	ptr, err := BytePtrFromString(value)
	if err != nil {
		t.Fatalf("BytePtrFromString(%q) failed: %v", value, err)
	}
	return ptr
}

func metadataStringFunc(t *testing.T, want *opendalMetadata, ptr *byte) func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
	t.Helper()
	return func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		assertMetadataPointerFromArgs(t, want, aValues...)
		*(**byte)(rValue) = ptr
	}
}

func assertOptionalString(t *testing.T, name string, fn func() (string, bool), want string) {
	t.Helper()

	got, ok := fn()
	if !ok || got != want {
		t.Fatalf("%s() = %q, %v, want %q, true", name, got, ok, want)
	}
}

func assertStringMap(t *testing.T, got, want map[string]string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("map length = %d, want %d: %v", len(got), len(want), got)
	}
	for key, wantValue := range want {
		if gotValue, ok := got[key]; !ok || gotValue != wantValue {
			t.Fatalf("map[%q] = %q, %v, want %q, true", key, gotValue, ok, wantValue)
		}
	}
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

func assertMetadataPointer(t *testing.T, want *opendalMetadata, got *opendalMetadata) {
	t.Helper()

	if got != want {
		t.Fatalf("metadata getter received %p, want %p", got, want)
	}
}

func assertMetadataPointerFromArgs(t *testing.T, want *opendalMetadata, aValues ...unsafe.Pointer) {
	t.Helper()

	if len(aValues) != 1 {
		t.Fatalf("metadata getter received %d arguments, want 1", len(aValues))
	}
	got := *(**opendalMetadata)(aValues[0])
	assertMetadataPointer(t, want, got)
}

func assertUserMetadataPointerFromArgs(t *testing.T, want *opendalMetadataUserMetadata, aValues ...unsafe.Pointer) {
	t.Helper()

	if len(aValues) != 1 {
		t.Fatalf("user metadata getter received %d arguments, want 1", len(aValues))
	}
	got := *(**opendalMetadataUserMetadata)(aValues[0])
	if got != want {
		t.Fatalf("user metadata getter received %p, want %p", got, want)
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

func TestWriteWithOptions(t *testing.T) {
	o := &writeOptions{}
	WriteWithAppend(true)(o)
	WriteWithCacheControl("max-age=60")(o)
	WriteWithContentType("text/plain")(o)
	WriteWithContentDisposition("attachment")(o)
	WriteWithContentEncoding("gzip")(o)
	WriteWithUserMetadata(map[string]string{"foo": "bar"})(o)
	WriteWithIfMatch("etag-a")(o)
	WriteWithIfNoneMatch("etag-b")(o)
	WriteWithIfNotExists(true)(o)
	WriteWithConcurrent(4)(o)
	WriteWithChunk(1024)(o)

	if !o.append {
		t.Fatalf("append = false, want true")
	}
	if o.cacheControl != "max-age=60" {
		t.Fatalf("cacheControl = %q, want max-age=60", o.cacheControl)
	}
	if o.contentType != "text/plain" {
		t.Fatalf("contentType = %q, want text/plain", o.contentType)
	}
	if o.contentDisposition != "attachment" {
		t.Fatalf("contentDisposition = %q, want attachment", o.contentDisposition)
	}
	if o.contentEncoding != "gzip" {
		t.Fatalf("contentEncoding = %q, want gzip", o.contentEncoding)
	}
	assertStringMap(t, o.userMetadata, map[string]string{"foo": "bar"})
	if o.ifMatch != "etag-a" {
		t.Fatalf("ifMatch = %q, want etag-a", o.ifMatch)
	}
	if o.ifNoneMatch != "etag-b" {
		t.Fatalf("ifNoneMatch = %q, want etag-b", o.ifNoneMatch)
	}
	if !o.ifNotExists {
		t.Fatalf("ifNotExists = false, want true")
	}
	if o.concurrent != 4 {
		t.Fatalf("concurrent = %d, want 4", o.concurrent)
	}
	if o.chunk != 1024 {
		t.Fatalf("chunk = %d, want 1024", o.chunk)
	}
}

func TestFfiOperatorWriteWithOptionsCancelArgTypes(t *testing.T) {
	aTypes := ffiOperatorWriteWithOptionsCancel.opts.aTypes
	if len(aTypes) != 5 {
		t.Fatalf("ffiOperatorWriteWithOptionsCancel aTypes len = %d, want 5", len(aTypes))
	}
	for i, at := range aTypes {
		if at != &ffi.TypePointer {
			t.Fatalf("ffiOperatorWriteWithOptionsCancel aTypes[%d] = %v, want TypePointer", i, at)
		}
	}
}

func TestFfiOperatorWriterWithOptionsCancelArgTypes(t *testing.T) {
	aTypes := ffiOperatorWriterWithOptionsCancel.opts.aTypes
	if len(aTypes) != 4 {
		t.Fatalf("ffiOperatorWriterWithOptionsCancel aTypes len = %d, want 4", len(aTypes))
	}
	for i, at := range aTypes {
		if at != &ffi.TypePointer {
			t.Fatalf("ffiOperatorWriterWithOptionsCancel aTypes[%d] = %v, want TypePointer", i, at)
		}
	}
}

func TestWriteOptionsSetterArgTypes(t *testing.T) {
	stringSetters := []*FFI[func(*opendalWriteOptions, string) ([]byte, error)]{
		ffiWriteOptionsSetCacheControl,
		ffiWriteOptionsSetContentType,
		ffiWriteOptionsSetContentDisposition,
		ffiWriteOptionsSetContentEncoding,
		ffiWriteOptionsSetIfMatch,
		ffiWriteOptionsSetIfNoneMatch,
	}
	for _, setter := range stringSetters {
		aTypes := setter.opts.aTypes
		if len(aTypes) != 2 {
			t.Fatalf("%s aTypes len = %d, want 2", setter.opts.sym, len(aTypes))
		}
		for i, at := range aTypes {
			if at != &ffi.TypePointer {
				t.Fatalf("%s aTypes[%d] = %v, want TypePointer", setter.opts.sym, i, at)
			}
		}
	}

	boolSetters := []*FFI[func(*opendalWriteOptions, bool)]{
		ffiWriteOptionsSetAppend,
		ffiWriteOptionsSetIfNotExists,
	}
	for _, setter := range boolSetters {
		aTypes := setter.opts.aTypes
		if len(aTypes) != 2 {
			t.Fatalf("%s aTypes len = %d, want 2", setter.opts.sym, len(aTypes))
		}
		if aTypes[0] != &ffi.TypePointer || aTypes[1] != &ffi.TypeUint8 {
			t.Fatalf("%s aTypes = %v, want TypePointer, TypeUint8", setter.opts.sym, aTypes)
		}
	}

	uintSetters := []*FFI[func(*opendalWriteOptions, uint)]{
		ffiWriteOptionsSetConcurrent,
		ffiWriteOptionsSetChunk,
	}
	for _, setter := range uintSetters {
		aTypes := setter.opts.aTypes
		if len(aTypes) != 2 {
			t.Fatalf("%s aTypes len = %d, want 2", setter.opts.sym, len(aTypes))
		}
		for i, at := range aTypes {
			if at != &ffi.TypePointer {
				t.Fatalf("%s aTypes[%d] = %v, want TypePointer", setter.opts.sym, i, at)
			}
		}
	}
}

func TestWriteOptionsSetUserMetadataArgTypes(t *testing.T) {
	aTypes := ffiWriteOptionsSetUserMetadata.opts.aTypes
	if len(aTypes) != 3 {
		t.Fatalf("ffiWriteOptionsSetUserMetadata aTypes len = %d, want 3", len(aTypes))
	}
	for i, at := range aTypes {
		if at != &ffi.TypePointer {
			t.Fatalf("ffiWriteOptionsSetUserMetadata aTypes[%d] = %v, want TypePointer", i, at)
		}
	}
}

func TestListWithRecursiveDefaultNotRecursive(t *testing.T) {
	o := &listOptions{}
	if o.recursive {
		t.Fatalf("default listOptions.recursive = true, want false")
	}
}

func TestListWithRecursiveTrue(t *testing.T) {
	o := &listOptions{}
	ListWithRecursive(true)(o)
	if !o.recursive {
		t.Fatalf("ListWithRecursive(true): recursive = false, want true")
	}
}

func TestListWithRecursiveFalse(t *testing.T) {
	o := &listOptions{}
	ListWithRecursive(true)(o)
	ListWithRecursive(false)(o)
	if o.recursive {
		t.Fatalf("ListWithRecursive(false): recursive = true, want false")
	}
}

func TestListWithLimitSetsValue(t *testing.T) {
	o := &listOptions{}
	ListWithLimit(100)(o)
	if o.limit != 100 {
		t.Fatalf("ListWithLimit(100): limit = %d, want 100", o.limit)
	}
}

func TestListWithLimitOverwrite(t *testing.T) {
	o := &listOptions{}
	ListWithLimit(50)(o)
	ListWithLimit(200)(o)
	if o.limit != 200 {
		t.Fatalf("ListWithLimit overwrite: limit = %d, want 200", o.limit)
	}
}

func TestListWithStartAfterSetsValue(t *testing.T) {
	o := &listOptions{}
	ListWithStartAfter("some/key")(o)
	if o.startAfter == nil {
		t.Fatal("ListWithStartAfter: startAfter = nil, want non-nil")
	}
	if *o.startAfter != "some/key" {
		t.Fatalf("ListWithStartAfter: startAfter = %q, want some/key", *o.startAfter)
	}
}

func TestListWithStartAfterEmptyString(t *testing.T) {
	o := &listOptions{}
	ListWithStartAfter("")(o)
	if o.startAfter == nil {
		t.Fatal("ListWithStartAfter(empty): startAfter = nil, want non-nil pointer to empty string")
	}
}

func TestListWithVersionsTrue(t *testing.T) {
	o := &listOptions{}
	ListWithVersions(true)(o)
	if !o.versions {
		t.Fatalf("ListWithVersions(true): versions = false, want true")
	}
}

func TestListWithVersionsFalse(t *testing.T) {
	o := &listOptions{}
	ListWithVersions(true)(o)
	ListWithVersions(false)(o)
	if o.versions {
		t.Fatalf("ListWithVersions(false): versions = true, want false")
	}
}

func TestListWithDeletedTrue(t *testing.T) {
	o := &listOptions{}
	ListWithDeleted(true)(o)
	if !o.deleted {
		t.Fatalf("ListWithDeleted(true): deleted = false, want true")
	}
}

func TestListWithDeletedFalse(t *testing.T) {
	o := &listOptions{}
	ListWithDeleted(true)(o)
	ListWithDeleted(false)(o)
	if o.deleted {
		t.Fatalf("ListWithDeleted(false): deleted = true, want false")
	}
}

func TestFfiOperatorListWithOptionsCancelReturnType(t *testing.T) {
	if ffiOperatorListWithOptionsCancel.opts.rType != &typeResultList {
		t.Fatalf("ffiOperatorListWithOptionsCancel rType = %v, want typeResultList", ffiOperatorListWithOptionsCancel.opts.rType)
	}
}

func TestFfiOperatorListWithOptionsCancelArgTypes(t *testing.T) {
	aTypes := ffiOperatorListWithOptionsCancel.opts.aTypes
	if len(aTypes) != 4 {
		t.Fatalf("ffiOperatorListWithOptionsCancel aTypes len = %d, want 4", len(aTypes))
	}
	for i, at := range aTypes {
		if at != &ffi.TypePointer {
			t.Fatalf("ffiOperatorListWithOptionsCancel aTypes[%d] = %v, want TypePointer", i, at)
		}
	}
}

func TestReadWithOptions(t *testing.T) {
	modified := time.Unix(1700000000, 0)
	unmodified := time.Unix(1700000123, 0)

	o := &readOptions{}
	ReadWithRange(1024, 2048)(o)
	ReadWithVersion("v1")(o)
	ReadWithIfMatch("etag-a")(o)
	ReadWithIfNoneMatch("etag-b")(o)
	ReadWithIfModifiedSince(modified)(o)
	ReadWithIfUnmodifiedSince(unmodified)(o)
	ReadWithConcurrent(4)(o)
	ReadWithChunk(1024)(o)
	ReadWithGap(512)(o)
	ReadWithOverrideContentType("text/plain")(o)
	ReadWithOverrideCacheControl("max-age=60")(o)
	ReadWithOverrideContentDisposition("attachment")(o)

	if !o.hasRange {
		t.Fatalf("hasRange = false, want true")
	}
	if o.rangeOffset != 1024 {
		t.Fatalf("rangeOffset = %d, want 1024", o.rangeOffset)
	}
	if o.rangeLength != 2048 {
		t.Fatalf("rangeLength = %d, want 2048", o.rangeLength)
	}
	if o.version != "v1" {
		t.Fatalf("version = %q, want v1", o.version)
	}
	if o.ifMatch != "etag-a" {
		t.Fatalf("ifMatch = %q, want etag-a", o.ifMatch)
	}
	if o.ifNoneMatch != "etag-b" {
		t.Fatalf("ifNoneMatch = %q, want etag-b", o.ifNoneMatch)
	}
	if o.ifModifiedSince == nil || *o.ifModifiedSince != modified.UnixMilli() {
		t.Fatalf("ifModifiedSince = %v, want %d", o.ifModifiedSince, modified.UnixMilli())
	}
	if o.ifUnmodifiedSince == nil || *o.ifUnmodifiedSince != unmodified.UnixMilli() {
		t.Fatalf("ifUnmodifiedSince = %v, want %d", o.ifUnmodifiedSince, unmodified.UnixMilli())
	}
	if o.concurrent != 4 {
		t.Fatalf("concurrent = %d, want 4", o.concurrent)
	}
	if o.chunk != 1024 {
		t.Fatalf("chunk = %d, want 1024", o.chunk)
	}
	if o.gap != 512 {
		t.Fatalf("gap = %d, want 512", o.gap)
	}
	if o.overrideContentType != "text/plain" {
		t.Fatalf("overrideContentType = %q, want text/plain", o.overrideContentType)
	}
	if o.overrideCacheControl != "max-age=60" {
		t.Fatalf("overrideCacheControl = %q, want max-age=60", o.overrideCacheControl)
	}
	if o.overrideContentDisposition != "attachment" {
		t.Fatalf("overrideContentDisposition = %q, want attachment", o.overrideContentDisposition)
	}
}

func TestFfiOperatorReadWithOptionsCancelReturnType(t *testing.T) {
	if ffiOperatorReadWithOptionsCancel.opts.rType != &typeResultRead {
		t.Fatalf("ffiOperatorReadWithOptionsCancel rType = %v, want typeResultRead", ffiOperatorReadWithOptionsCancel.opts.rType)
	}
}

func TestFfiOperatorReadWithOptionsCancelArgTypes(t *testing.T) {
	aTypes := ffiOperatorReadWithOptionsCancel.opts.aTypes
	if len(aTypes) != 4 {
		t.Fatalf("ffiOperatorReadWithOptionsCancel aTypes len = %d, want 4", len(aTypes))
	}
	for i, at := range aTypes {
		if at != &ffi.TypePointer {
			t.Fatalf("ffiOperatorReadWithOptionsCancel aTypes[%d] = %v, want TypePointer", i, at)
		}
	}
}

func TestReadOptionsSetterArgTypes(t *testing.T) {
	stringSetters := []*FFI[func(*opendalReadOptions, string) ([]byte, error)]{
		ffiReadOptionsSetVersion,
		ffiReadOptionsSetIfMatch,
		ffiReadOptionsSetIfNoneMatch,
		ffiReadOptionsSetOverrideContentType,
		ffiReadOptionsSetOverrideCacheControl,
		ffiReadOptionsSetOverrideContentDisposition,
	}
	for _, setter := range stringSetters {
		aTypes := setter.opts.aTypes
		if len(aTypes) != 2 {
			t.Fatalf("%s aTypes len = %d, want 2", setter.opts.sym, len(aTypes))
		}
		for i, at := range aTypes {
			if at != &ffi.TypePointer {
				t.Fatalf("%s aTypes[%d] = %v, want TypePointer", setter.opts.sym, i, at)
			}
		}
	}

	int64Setters := []*FFI[func(*opendalReadOptions, int64)]{
		ffiReadOptionsSetIfModifiedSince,
		ffiReadOptionsSetIfUnmodifiedSince,
	}
	for _, setter := range int64Setters {
		aTypes := setter.opts.aTypes
		if len(aTypes) != 2 {
			t.Fatalf("%s aTypes len = %d, want 2", setter.opts.sym, len(aTypes))
		}
		if aTypes[0] != &ffi.TypePointer || aTypes[1] != &ffi.TypeSint64 {
			t.Fatalf("%s aTypes = %v, want TypePointer, TypeSint64", setter.opts.sym, aTypes)
		}
	}

	uintSetters := []*FFI[func(*opendalReadOptions, uint)]{
		ffiReadOptionsSetConcurrent,
		ffiReadOptionsSetChunk,
		ffiReadOptionsSetGap,
	}
	for _, setter := range uintSetters {
		aTypes := setter.opts.aTypes
		if len(aTypes) != 2 {
			t.Fatalf("%s aTypes len = %d, want 2", setter.opts.sym, len(aTypes))
		}
		for i, at := range aTypes {
			if at != &ffi.TypePointer {
				t.Fatalf("%s aTypes[%d] = %v, want TypePointer", setter.opts.sym, i, at)
			}
		}
	}

	rangeATypes := ffiReadOptionsSetRange.opts.aTypes
	if len(rangeATypes) != 3 {
		t.Fatalf("ffiReadOptionsSetRange aTypes len = %d, want 3", len(rangeATypes))
	}
	if rangeATypes[0] != &ffi.TypePointer || rangeATypes[1] != &ffi.TypeUint64 || rangeATypes[2] != &ffi.TypeUint64 {
		t.Fatalf("ffiReadOptionsSetRange aTypes = %v, want TypePointer, TypeUint64, TypeUint64", rangeATypes)
	}
}
