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
	"net/http"
	"testing"
	"time"
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
	capability := opendalCapability{stat: 1}
	infoFreed := 0

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiStringFree.sym, freeCString)
	ctx = context.WithValue(ctx, ffiOperatorInfoNew.sym, func(op *opendalOperator) *opendalOperatorInfo {
		if op == nil {
			t.Fatal("Info() passed nil operator")
		}
		return infoInner
	})
	ctx = context.WithValue(ctx, ffiOperatorInfoFree.sym, func(info *opendalOperatorInfo) {
		if info != infoInner {
			t.Fatalf("Info() freed unexpected operator info: %p", info)
		}
		infoFreed++
	})
	ctx = context.WithValue(ctx, ffiOperatorInfoGetScheme.sym, ffiOperatorInfoGetScheme.withFunc(ctx, func(info *opendalOperatorInfo) *byte {
		assertOperatorInfoPointer(t, infoInner, info)
		return schemePtr
	}))
	ctx = context.WithValue(ctx, ffiOperatorInfoGetRoot.sym, ffiOperatorInfoGetRoot.withFunc(ctx, func(info *opendalOperatorInfo) *byte {
		assertOperatorInfoPointer(t, infoInner, info)
		return rootPtr
	}))
	ctx = context.WithValue(ctx, ffiOperatorInfoGetName.sym, ffiOperatorInfoGetName.withFunc(ctx, func(info *opendalOperatorInfo) *byte {
		assertOperatorInfoPointer(t, infoInner, info)
		return namePtr
	}))
	ctx = context.WithValue(ctx, ffiOperatorInfoGetCapability.sym, ffiOperatorInfoGetCapability.withFunc(ctx, func(info *opendalOperatorInfo, cap *opendalCapability) {
		assertOperatorInfoPointer(t, infoInner, info)
		if cap == nil {
			t.Fatal("Info() passed nil capability output pointer")
		}
		*cap = capability
	}))

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
	if !info.GetCapability().Stat() {
		t.Fatal("Info().GetCapability().Stat() = false, want true")
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
	ctx = context.WithValue(ctx, ffiStringFree.sym, freeCString)
	ctx = context.WithValue(ctx, ffiEntryFree.sym, func(entry *opendalEntry) {
		if entry != entryInner {
			t.Fatalf("newEntry() freed unexpected entry: %p", entry)
		}
		entryFreed++
	})
	ctx = context.WithValue(ctx, ffiEntryName.sym, ffiEntryName.withFunc(ctx, func(entry *opendalEntry) *byte {
		assertEntryPointer(t, entryInner, entry)
		return namePtr
	}))
	ctx = context.WithValue(ctx, ffiEntryPath.sym, ffiEntryPath.withFunc(ctx, func(entry *opendalEntry) *byte {
		assertEntryPointer(t, entryInner, entry)
		return pathPtr
	}))

	// Mock the metadata path exercised by newEntry()
	metaInner := &opendalMetadata{}
	metaFreed := 0
	ctx = context.WithValue(ctx, ffiEntryMetadata.sym, func(e *opendalEntry) *opendalMetadata {
		if e != entryInner {
			t.Fatalf("ffiEntryMetadata called with unexpected entry")
		}
		return metaInner
	})
	ctx = context.WithValue(ctx, ffiMetadataFree.sym, func(m *opendalMetadata) {
		if m != metaInner {
			t.Fatalf("metadata freed unexpected pointer: %p", m)
		}
		metaFreed++
	})
	ctx = context.WithValue(ctx, ffiMetaCacheControl.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentDisposition.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentEncoding.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentLength.sym, func(m *opendalMetadata) uint64 {
		assertMetadataPointer(t, metaInner, m)
		return 4096
	})
	ctx = context.WithValue(ctx, ffiMetaContentMD5.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	contentType := "text/plain"
	ctx = context.WithValue(ctx, ffiMetaContentType.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return &contentType
	})
	ctx = context.WithValue(ctx, ffiMetaEtag.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaIsCurrent.sym, func(m *opendalMetadata) uint8 {
		assertMetadataPointer(t, metaInner, m)
		return 2
	})
	ctx = context.WithValue(ctx, ffiMetaIsDeleted.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metaInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsFile.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metaInner, m)
		return true
	})
	ctx = context.WithValue(ctx, ffiMetaIsDir.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metaInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaLastModified.sym, func(m *opendalMetadata) int64 {
		assertMetadataPointer(t, metaInner, m)
		return 1700000000000
	})
	ctx = context.WithValue(ctx, ffiMetaMode.sym, func(m *opendalMetadata) EntryMode {
		assertMetadataPointer(t, metaInner, m)
		return EntryModeFile
	})
	ctx = context.WithValue(ctx, ffiMetaVersion.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metaInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadata.sym, func(m *opendalMetadata) map[string]string {
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

	// Verify Metadata() support
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
	ctx = context.WithValue(ctx, ffiStringFree.sym, freeCString)
	ctx = context.WithValue(ctx, ffiMetadataFree.sym, func(m *opendalMetadata) {
		assertMetadataPointer(t, metadataInner, m)
		metadataFreed++
	})
	ctx = context.WithValue(ctx, ffiMetaCacheControl.sym, ffiMetaCacheControl.withFunc(ctx, metadataStringFunc(t, metadataInner, cacheControlPtr)))
	ctx = context.WithValue(ctx, ffiMetaContentDisposition.sym, ffiMetaContentDisposition.withFunc(ctx, metadataStringFunc(t, metadataInner, contentDispositionPtr)))
	ctx = context.WithValue(ctx, ffiMetaContentEncoding.sym, ffiMetaContentEncoding.withFunc(ctx, metadataStringFunc(t, metadataInner, contentEncodingPtr)))
	ctx = context.WithValue(ctx, ffiMetaContentLength.sym, func(m *opendalMetadata) uint64 {
		assertMetadataPointer(t, metadataInner, m)
		return 4096
	})
	ctx = context.WithValue(ctx, ffiMetaContentMD5.sym, ffiMetaContentMD5.withFunc(ctx, metadataStringFunc(t, metadataInner, contentMD5Ptr)))
	ctx = context.WithValue(ctx, ffiMetaContentType.sym, ffiMetaContentType.withFunc(ctx, metadataStringFunc(t, metadataInner, contentTypePtr)))
	ctx = context.WithValue(ctx, ffiMetaEtag.sym, ffiMetaEtag.withFunc(ctx, metadataStringFunc(t, metadataInner, etagPtr)))
	ctx = context.WithValue(ctx, ffiMetaIsCurrent.sym, func(m *opendalMetadata) uint8 {
		assertMetadataPointer(t, metadataInner, m)
		return 1
	})
	ctx = context.WithValue(ctx, ffiMetaIsDeleted.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return true
	})
	ctx = context.WithValue(ctx, ffiMetaIsFile.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsDir.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return true
	})
	ctx = context.WithValue(ctx, ffiMetaLastModified.sym, func(m *opendalMetadata) int64 {
		assertMetadataPointer(t, metadataInner, m)
		return 1700000000000
	})
	ctx = context.WithValue(ctx, ffiMetaMode.sym, func(m *opendalMetadata) EntryMode {
		assertMetadataPointer(t, metadataInner, m)
		return EntryModeDir
	})
	ctx = context.WithValue(ctx, ffiMetaVersion.sym, ffiMetaVersion.withFunc(ctx, metadataStringFunc(t, metadataInner, versionPtr)))
	ctx = context.WithValue(ctx, ffiMetaUserMetadataPairs.sym, func(m *opendalMetadataUserMetadata) *opendalMetadataUserMetadataPair {
		assertUserMetadataPointer(t, userMetadataInner, m)
		return &pairs[0]
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadataLen.sym, func(m *opendalMetadataUserMetadata) uintptr {
		assertUserMetadataPointer(t, userMetadataInner, m)
		return uintptr(len(pairs))
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadataFree.sym, func(m *opendalMetadataUserMetadata) {
		assertUserMetadataPointer(t, userMetadataInner, m)
		userMetadataFreed++
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadata.sym, ffiMetaUserMetadata.withFunc(ctx, func(m *opendalMetadata) *opendalMetadataUserMetadata {
		assertMetadataPointer(t, metadataInner, m)
		return userMetadataInner
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
	ctx = context.WithValue(ctx, ffiMetadataFree.sym, func(m *opendalMetadata) {
		assertMetadataPointer(t, metadataInner, m)
		metadataFreed++
	})
	ctx = context.WithValue(ctx, ffiMetaCacheControl.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentDisposition.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentEncoding.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentLength.sym, func(m *opendalMetadata) uint64 {
		assertMetadataPointer(t, metadataInner, m)
		return 0
	})
	ctx = context.WithValue(ctx, ffiMetaContentMD5.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentType.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaEtag.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaIsCurrent.sym, func(m *opendalMetadata) uint8 {
		assertMetadataPointer(t, metadataInner, m)
		return 2
	})
	ctx = context.WithValue(ctx, ffiMetaIsDeleted.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsFile.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsDir.sym, func(m *opendalMetadata) bool {
		assertMetadataPointer(t, metadataInner, m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaLastModified.sym, func(m *opendalMetadata) int64 {
		assertMetadataPointer(t, metadataInner, m)
		return -1
	})
	ctx = context.WithValue(ctx, ffiMetaMode.sym, func(m *opendalMetadata) EntryMode {
		assertMetadataPointer(t, metadataInner, m)
		return EntryModeUnknown
	})
	ctx = context.WithValue(ctx, ffiMetaVersion.sym, func(m *opendalMetadata) *string {
		assertMetadataPointer(t, metadataInner, m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadata.sym, func(m *opendalMetadata) map[string]string {
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

func metadataStringFunc(t *testing.T, want *opendalMetadata, ptr *byte) func(m *opendalMetadata) *byte {
	t.Helper()
	return func(m *opendalMetadata) *byte {
		assertMetadataPointer(t, want, m)
		return ptr
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

func assertOperatorInfoPointer(t *testing.T, want *opendalOperatorInfo, got *opendalOperatorInfo) {
	t.Helper()

	if got != want {
		t.Fatalf("operator info getter received %p, want %p", got, want)
	}
}

func assertEntryPointer(t *testing.T, want *opendalEntry, got *opendalEntry) {
	t.Helper()

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

func assertUserMetadataPointer(t *testing.T, want *opendalMetadataUserMetadata, got *opendalMetadataUserMetadata) {
	t.Helper()

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
	ReadWithContentLengthHint(4096)(o)
	ReadWithOverrideContentType("text/plain")(o)
	ReadWithOverrideCacheControl("max-age=60")(o)
	ReadWithOverrideContentDisposition("attachment")(o)

	if !o.hasRange {
		t.Fatalf("hasRange = false, want true")
	}
	if !o.hasRangeLength {
		t.Fatalf("hasRangeLength = false, want true")
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
	if o.contentLengthHint == nil || *o.contentLengthHint != 4096 {
		t.Fatalf("contentLengthHint = %v, want 4096", o.contentLengthHint)
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

func TestReadWithRangeFrom(t *testing.T) {
	o := &readOptions{}
	ReadWithRangeFrom(1024)(o)

	if !o.hasRange {
		t.Fatalf("hasRange = false, want true")
	}
	if o.hasRangeLength {
		t.Fatalf("hasRangeLength = true, want false")
	}
	if o.rangeOffset != 1024 {
		t.Fatalf("rangeOffset = %d, want 1024", o.rangeOffset)
	}

	// ReadWithRangeFrom overrides a previously set bounded range.
	o = &readOptions{}
	ReadWithRange(0, 2048)(o)
	ReadWithRangeFrom(512)(o)
	if o.hasRangeLength || o.rangeOffset != 512 || o.rangeLength != 0 {
		t.Fatalf("after override: hasRangeLength=%v offset=%d length=%d, want false 512 0",
			o.hasRangeLength, o.rangeOffset, o.rangeLength)
	}
}

func TestReaderWithOptions(t *testing.T) {
	modified := time.Unix(1700000000, 0)
	unmodified := time.Unix(1700000123, 0)

	o := &readerOptions{}
	ReaderWithVersion("v1")(o)
	ReaderWithIfMatch("etag-a")(o)
	ReaderWithIfNoneMatch("etag-b")(o)
	ReaderWithIfModifiedSince(modified)(o)
	ReaderWithIfUnmodifiedSince(unmodified)(o)
	ReaderWithContentLengthHint(4096)(o)
	ReaderWithConcurrent(4)(o)
	ReaderWithChunk(1024)(o)
	ReaderWithGap(512)(o)
	ReaderWithPrefetch(8)(o)

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
	if o.contentLengthHint == nil || *o.contentLengthHint != 4096 {
		t.Fatalf("contentLengthHint = %v, want 4096", o.contentLengthHint)
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
	if o.prefetch != 8 {
		t.Fatalf("prefetch = %d, want 8", o.prefetch)
	}
}

func TestPresignWithoutOptionsUsesSimpleSymbols(t *testing.T) {
	ctx, reqInner, freeCount := presignTestContext(t, "GET", "https://example.com/simple")
	opInner := &opendalOperator{}
	op := &Operator{ctx: ctx, inner: opInner}

	ctx = context.WithValue(ctx, ffiOperatorPresignRead.sym, func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
		assertPresignCall(t, opInner, op, "file.txt", path, uint64(60), expire)
		return reqInner, nil
	})
	ctx = context.WithValue(ctx, ffiOperatorPresignWrite.sym, func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
		assertPresignCall(t, opInner, op, "file.txt", path, uint64(60), expire)
		return reqInner, nil
	})
	ctx = context.WithValue(ctx, ffiOperatorPresignDelete.sym, func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
		assertPresignCall(t, opInner, op, "file.txt", path, uint64(60), expire)
		return reqInner, nil
	})
	ctx = context.WithValue(ctx, ffiOperatorPresignStat.sym, func(op *opendalOperator, path string, expire uint64) (*opendalPresignedRequest, error) {
		assertPresignCall(t, opInner, op, "file.txt", path, uint64(60), expire)
		return reqInner, nil
	})
	op.ctx = ctx

	for name, call := range map[string]func() (*http.Request, error){
		"read":   func() (*http.Request, error) { return op.PresignRead("file.txt", time.Minute) },
		"write":  func() (*http.Request, error) { return op.PresignWrite("file.txt", time.Minute) },
		"delete": func() (*http.Request, error) { return op.PresignDelete("file.txt", time.Minute) },
		"stat":   func() (*http.Request, error) { return op.PresignStat("file.txt", time.Minute) },
	} {
		req, err := call()
		if err != nil {
			t.Fatalf("Presign%s() failed: %v", name, err)
		}
		if req.Method != "GET" || req.URL.String() != "https://example.com/simple" {
			t.Fatalf("Presign%s() request = %s %s", name, req.Method, req.URL.String())
		}
	}
	if *freeCount != 4 {
		t.Fatalf("presigned request freed %d times, want 4", *freeCount)
	}
}

func TestPresignReadWithOptionsKeepsStringsUntilCall(t *testing.T) {
	ctx, reqInner, _ := presignTestContext(t, "GET", "https://example.com/read")
	opInner := &opendalOperator{}
	cOpts := &opendalReadOptions{}
	var versionPtr *byte
	var overrideContentTypePtr *byte
	var rangeOffset, rangeLength uint64

	ctx = context.WithValue(ctx, ffiReadOptionsNew.sym, func() *opendalReadOptions { return cOpts })
	ctx = context.WithValue(ctx, ffiReadOptionsFree.sym, func(opts *opendalReadOptions) {
		assertReadOptionsPointer(t, cOpts, opts)
	})
	ctx = context.WithValue(ctx, ffiReadOptionsSetRange.sym, func(opts *opendalReadOptions, offset, length uint64) {
		assertReadOptionsPointer(t, cOpts, opts)
		rangeOffset = offset
		rangeLength = length
	})
	ctx = context.WithValue(ctx, ffiReadOptionsSetVersion.sym, ffiReadOptionsSetVersion.withFunc(ctx, func(opts *opendalReadOptions, value *byte) {
		assertReadOptionsPointer(t, cOpts, opts)
		versionPtr = value
	}))
	ctx = context.WithValue(ctx, ffiReadOptionsSetIfMatch.sym, noopReadOptionsSetString)
	ctx = context.WithValue(ctx, ffiReadOptionsSetIfNoneMatch.sym, noopReadOptionsSetString)
	ctx = context.WithValue(ctx, ffiReadOptionsSetOverrideContentType.sym, ffiReadOptionsSetOverrideContentType.withFunc(ctx, func(opts *opendalReadOptions, value *byte) {
		assertReadOptionsPointer(t, cOpts, opts)
		overrideContentTypePtr = value
	}))
	ctx = context.WithValue(ctx, ffiReadOptionsSetOverrideCacheControl.sym, noopReadOptionsSetString)
	ctx = context.WithValue(ctx, ffiReadOptionsSetOverrideContentDisposition.sym, noopReadOptionsSetString)
	ctx = context.WithValue(ctx, ffiOperatorPresignReadWith.sym, ffiOperatorPresignReadWith.withFunc(ctx, func(op *opendalOperator, path *byte, expire uint64, opts *opendalReadOptions, req **opendalPresignedRequest) *opendalError {
		assertPresignCall(t, opInner, op, "file.txt", BytePtrToString(path), uint64(60), expire)
		assertReadOptionsPointer(t, cOpts, opts)
		if rangeOffset != 1 || rangeLength != 2 {
			t.Fatalf("read range = %d, %d, want 1, 2", rangeOffset, rangeLength)
		}
		if got := BytePtrToString(versionPtr); got != "v1" {
			t.Fatalf("read version pointer = %q, want v1", got)
		}
		if got := BytePtrToString(overrideContentTypePtr); got != "text/plain" {
			t.Fatalf("read override content type pointer = %q, want text/plain", got)
		}
		*req = reqInner
		return nil
	}))

	op := &Operator{ctx: ctx, inner: opInner}
	_, err := op.PresignRead("file.txt", time.Minute,
		ReadWithRange(1, 2),
		ReadWithVersion("v1"),
		ReadWithOverrideContentType("text/plain"),
	)
	if err != nil {
		t.Fatalf("PresignRead with options failed: %v", err)
	}
}

func TestPresignWriteWithOptionsKeepsStringsUntilCall(t *testing.T) {
	ctx, reqInner, _ := presignTestContext(t, "PUT", "https://example.com/write")
	opInner := &opendalOperator{}
	cOpts := &opendalWriteOptions{}
	var contentTypePtr *byte
	var userMetadata []opendalWriteUserMetadataPair

	ctx = context.WithValue(ctx, ffiWriteOptionsNew.sym, func() *opendalWriteOptions { return cOpts })
	ctx = context.WithValue(ctx, ffiWriteOptionsFree.sym, func(opts *opendalWriteOptions) {
		assertWriteOptionsPointer(t, cOpts, opts)
	})
	ctx = context.WithValue(ctx, ffiWriteOptionsSetAppend.sym, func(opts *opendalWriteOptions, append bool) {
		assertWriteOptionsPointer(t, cOpts, opts)
	})
	ctx = context.WithValue(ctx, ffiWriteOptionsSetIfNotExists.sym, func(opts *opendalWriteOptions, ifNotExists bool) {
		assertWriteOptionsPointer(t, cOpts, opts)
	})
	ctx = context.WithValue(ctx, ffiWriteOptionsSetContentType.sym, ffiWriteOptionsSetContentType.withFunc(ctx, func(opts *opendalWriteOptions, value *byte) {
		assertWriteOptionsPointer(t, cOpts, opts)
		contentTypePtr = value
	}))
	ctx = context.WithValue(ctx, ffiWriteOptionsSetCacheControl.sym, noopWriteOptionsSetString)
	ctx = context.WithValue(ctx, ffiWriteOptionsSetContentDisposition.sym, noopWriteOptionsSetString)
	ctx = context.WithValue(ctx, ffiWriteOptionsSetContentEncoding.sym, noopWriteOptionsSetString)
	ctx = context.WithValue(ctx, ffiWriteOptionsSetIfMatch.sym, noopWriteOptionsSetString)
	ctx = context.WithValue(ctx, ffiWriteOptionsSetIfNoneMatch.sym, noopWriteOptionsSetString)
	ctx = context.WithValue(ctx, ffiWriteOptionsSetUserMetadata.sym, func(opts *opendalWriteOptions, pairs []opendalWriteUserMetadataPair) {
		assertWriteOptionsPointer(t, cOpts, opts)
		userMetadata = pairs
	})
	ctx = context.WithValue(ctx, ffiOperatorPresignWriteWith.sym, ffiOperatorPresignWriteWith.withFunc(ctx, func(op *opendalOperator, path *byte, expire uint64, opts *opendalWriteOptions, req **opendalPresignedRequest) *opendalError {
		assertPresignCall(t, opInner, op, "file.txt", BytePtrToString(path), uint64(60), expire)
		assertWriteOptionsPointer(t, cOpts, opts)
		if got := BytePtrToString(contentTypePtr); got != "text/plain" {
			t.Fatalf("write content type pointer = %q, want text/plain", got)
		}
		if len(userMetadata) != 1 {
			t.Fatalf("write user metadata len = %d, want 1", len(userMetadata))
		}
		if got := BytePtrToString(userMetadata[0].key); got != "foo" {
			t.Fatalf("write user metadata key = %q, want foo", got)
		}
		if got := BytePtrToString(userMetadata[0].value); got != "bar" {
			t.Fatalf("write user metadata value = %q, want bar", got)
		}
		*req = reqInner
		return nil
	}))

	op := &Operator{ctx: ctx, inner: opInner}
	_, err := op.PresignWrite("file.txt", time.Minute,
		WriteWithContentType("text/plain"),
		WriteWithUserMetadata(map[string]string{"foo": "bar"}),
	)
	if err != nil {
		t.Fatalf("PresignWrite with options failed: %v", err)
	}
}

func TestPresignStatWithOptionsKeepsStringsUntilCall(t *testing.T) {
	ctx, reqInner, _ := presignTestContext(t, "HEAD", "https://example.com/stat")
	opInner := &opendalOperator{}
	cOpts := &opendalStatOptions{}
	var versionPtr *byte
	var overrideContentTypePtr *byte

	ctx = context.WithValue(ctx, ffiStatOptionsNew.sym, func() *opendalStatOptions { return cOpts })
	ctx = context.WithValue(ctx, ffiStatOptionsFree.sym, func(opts *opendalStatOptions) {
		assertStatOptionsPointer(t, cOpts, opts)
	})
	ctx = context.WithValue(ctx, ffiStatOptionsSetVersion.sym, ffiStatOptionsSetVersion.withFunc(ctx, func(opts *opendalStatOptions, value *byte) {
		assertStatOptionsPointer(t, cOpts, opts)
		versionPtr = value
	}))
	ctx = context.WithValue(ctx, ffiStatOptionsSetIfMatch.sym, noopStatOptionsSetString)
	ctx = context.WithValue(ctx, ffiStatOptionsSetIfNoneMatch.sym, noopStatOptionsSetString)
	ctx = context.WithValue(ctx, ffiStatOptionsSetOverrideContentType.sym, ffiStatOptionsSetOverrideContentType.withFunc(ctx, func(opts *opendalStatOptions, value *byte) {
		assertStatOptionsPointer(t, cOpts, opts)
		overrideContentTypePtr = value
	}))
	ctx = context.WithValue(ctx, ffiStatOptionsSetOverrideCacheControl.sym, noopStatOptionsSetString)
	ctx = context.WithValue(ctx, ffiStatOptionsSetOverrideContentDisposition.sym, noopStatOptionsSetString)
	ctx = context.WithValue(ctx, ffiOperatorPresignStatWith.sym, ffiOperatorPresignStatWith.withFunc(ctx, func(op *opendalOperator, path *byte, expire uint64, opts *opendalStatOptions, req **opendalPresignedRequest) *opendalError {
		assertPresignCall(t, opInner, op, "file.txt", BytePtrToString(path), uint64(60), expire)
		assertStatOptionsPointer(t, cOpts, opts)
		if got := BytePtrToString(versionPtr); got != "v1" {
			t.Fatalf("stat version pointer = %q, want v1", got)
		}
		if got := BytePtrToString(overrideContentTypePtr); got != "text/plain" {
			t.Fatalf("stat override content type pointer = %q, want text/plain", got)
		}
		*req = reqInner
		return nil
	}))

	op := &Operator{ctx: ctx, inner: opInner}
	_, err := op.PresignStat("file.txt", time.Minute,
		StatWithVersion("v1"),
		StatWithOverrideContentType("text/plain"),
	)
	if err != nil {
		t.Fatalf("PresignStat with options failed: %v", err)
	}
}

func TestPresignDeleteWithOptionsKeepsStringsUntilCall(t *testing.T) {
	ctx, reqInner, _ := presignTestContext(t, "DELETE", "https://example.com/delete")
	opInner := &opendalOperator{}
	cOpts := &opendalDeleteOptions{}
	var versionPtr *byte
	var recursive bool

	ctx = context.WithValue(ctx, ffiDeleteOptionsNew.sym, func() *opendalDeleteOptions { return cOpts })
	ctx = context.WithValue(ctx, ffiDeleteOptionsFree.sym, func(opts *opendalDeleteOptions) {
		assertDeleteOptionsPointer(t, cOpts, opts)
	})
	ctx = context.WithValue(ctx, ffiDeleteOptionsSetRecursive.sym, func(opts *opendalDeleteOptions, value bool) {
		assertDeleteOptionsPointer(t, cOpts, opts)
		recursive = value
	})
	ctx = context.WithValue(ctx, ffiDeleteOptionsSetVersion.sym, ffiDeleteOptionsSetVersion.withFunc(ctx, func(opts *opendalDeleteOptions, value *byte) {
		assertDeleteOptionsPointer(t, cOpts, opts)
		versionPtr = value
	}))
	ctx = context.WithValue(ctx, ffiOperatorPresignDeleteWith.sym, ffiOperatorPresignDeleteWith.withFunc(ctx, func(op *opendalOperator, path *byte, expire uint64, opts *opendalDeleteOptions, req **opendalPresignedRequest) *opendalError {
		assertPresignCall(t, opInner, op, "file.txt", BytePtrToString(path), uint64(60), expire)
		assertDeleteOptionsPointer(t, cOpts, opts)
		if !recursive {
			t.Fatal("delete recursive = false, want true")
		}
		if got := BytePtrToString(versionPtr); got != "v1" {
			t.Fatalf("delete version pointer = %q, want v1", got)
		}
		*req = reqInner
		return nil
	}))

	op := &Operator{ctx: ctx, inner: opInner}
	_, err := op.PresignDelete("file.txt", time.Minute,
		DeleteWithVersion("v1"),
		DeleteWithRecursive(true),
	)
	if err != nil {
		t.Fatalf("PresignDelete with options failed: %v", err)
	}
}

func presignTestContext(t *testing.T, method, uri string) (context.Context, *opendalPresignedRequest, *int) {
	t.Helper()

	methodData, err := byteSliceFromString(method)
	if err != nil {
		t.Fatalf("byteSliceFromString(%q) failed: %v", method, err)
	}
	uriData, err := byteSliceFromString(uri)
	if err != nil {
		t.Fatalf("byteSliceFromString(%q) failed: %v", uri, err)
	}
	reqInner := &opendalPresignedRequest{}
	freeCount := 0

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiPresignedRequestMethod.sym, func(req *opendalPresignedRequest) *byte {
		assertPresignedRequestPointer(t, reqInner, req)
		return &methodData[0]
	})
	ctx = context.WithValue(ctx, ffiPresignedRequestUri.sym, func(req *opendalPresignedRequest) *byte {
		assertPresignedRequestPointer(t, reqInner, req)
		return &uriData[0]
	})
	ctx = context.WithValue(ctx, ffiPresignedRequestHeaders.sym, func(req *opendalPresignedRequest) *opendalHttpHeaderPair {
		assertPresignedRequestPointer(t, reqInner, req)
		return nil
	})
	ctx = context.WithValue(ctx, ffiPresignedRequestHeadersLen.sym, func(req *opendalPresignedRequest) uintptr {
		assertPresignedRequestPointer(t, reqInner, req)
		return 0
	})
	ctx = context.WithValue(ctx, ffiPresignedRequestFree.sym, func(req *opendalPresignedRequest) {
		assertPresignedRequestPointer(t, reqInner, req)
		freeCount++
	})
	return ctx, reqInner, &freeCount
}

func assertPresignCall(t *testing.T, wantOp *opendalOperator, gotOp *opendalOperator, wantPath, gotPath string, wantExpire, gotExpire uint64) {
	t.Helper()
	if gotOp != wantOp {
		t.Fatalf("presign op = %p, want %p", gotOp, wantOp)
	}
	if gotPath != wantPath {
		t.Fatalf("presign path = %q, want %q", gotPath, wantPath)
	}
	if gotExpire != wantExpire {
		t.Fatalf("presign expire = %d, want %d", gotExpire, wantExpire)
	}
}

func assertPresignedRequestPointer(t *testing.T, want *opendalPresignedRequest, got *opendalPresignedRequest) {
	t.Helper()
	if got != want {
		t.Fatalf("presigned request = %p, want %p", got, want)
	}
}

func assertReadOptionsPointer(t *testing.T, want *opendalReadOptions, got *opendalReadOptions) {
	t.Helper()
	if got != want {
		t.Fatalf("read options = %p, want %p", got, want)
	}
}

func noopReadOptionsSetString(*opendalReadOptions, string) ([]byte, error) {
	return nil, nil
}

func assertWriteOptionsPointer(t *testing.T, want *opendalWriteOptions, got *opendalWriteOptions) {
	t.Helper()
	if got != want {
		t.Fatalf("write options = %p, want %p", got, want)
	}
}

func noopWriteOptionsSetString(*opendalWriteOptions, string) ([]byte, error) {
	return nil, nil
}

func assertStatOptionsPointer(t *testing.T, want *opendalStatOptions, got *opendalStatOptions) {
	t.Helper()
	if got != want {
		t.Fatalf("stat options = %p, want %p", got, want)
	}
}

func noopStatOptionsSetString(*opendalStatOptions, string) ([]byte, error) {
	return nil, nil
}

func assertDeleteOptionsPointer(t *testing.T, want *opendalDeleteOptions, got *opendalDeleteOptions) {
	t.Helper()
	if got != want {
		t.Fatalf("delete options = %p, want %p", got, want)
	}
}
