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
	"errors"
	"math"
	"net/http"
	"testing"
	"time"
	"unsafe"
)

func TestStringConversionsValidateUTF8AndNul(t *testing.T) {
	invalidUTF8 := []string{"\xff", "prefix\xfe\xffsuffix", string([]byte{0xc3, 0x28})}
	for _, s := range invalidUTF8 {
		if ptr, err := BytePtrFromString(s); err == nil || ptr != nil {
			t.Fatalf("BytePtrFromString(%q) = (%v, %v), want nil and error", s, ptr, err)
		}
		if data, err := byteSliceFromString(s); err == nil || data != nil {
			t.Fatalf("byteSliceFromString(%q) = (%v, %v), want nil and error", s, data, err)
		}
	}

	if ptr, err := BytePtrFromString("a\x00b"); err == nil || ptr != nil {
		t.Fatalf("BytePtrFromString(nul) = (%v, %v), want nil and error", ptr, err)
	}
	if data, err := byteSliceFromString("a\x00b"); err == nil || data != nil {
		t.Fatalf("byteSliceFromString(nul) = (%v, %v), want nil and error", data, err)
	}

	valid := []string{"", "hello.txt", "dir/世界.txt"}
	for _, s := range valid {
		ptr, err := BytePtrFromString(s)
		if err != nil || ptr == nil {
			t.Fatalf("BytePtrFromString(%q) = (%v, %v), want non-nil and nil error", s, ptr, err)
		}
		if got := BytePtrToString(ptr); got != s {
			t.Fatalf("BytePtrToString(BytePtrFromString(%q)) = %q", s, got)
		}
		data, err := byteSliceFromString(s)
		if err != nil || len(data) != len(s)+1 || data[len(s)] != 0 {
			t.Fatalf("byteSliceFromString(%q) = (%v, %v), want nul-terminated bytes", s, data, err)
		}
		if got := string(data[:len(s)]); got != s {
			t.Fatalf("byteSliceFromString(%q) content = %q", s, got)
		}
	}

	// Verify FFI wrappers reject invalid UTF-8 before invoking native functions.
	readCalled := false
	readFn := ffiOperatorRead.withFunc(context.Background(), func(*opendalOperator, *byte, *opendalReadOptions, *opendalCancelToken, *opendalBytes) *opendalError {
		readCalled = true
		return nil
	})
	if _, err := readFn(newTestOperatorHandle(), "\xff", nil, nil); err == nil {
		t.Fatal("ffiOperatorRead with invalid UTF-8 succeeded, want error")
	}
	if readCalled {
		t.Fatal("ffiOperatorRead invoked native symbol for invalid UTF-8 path")
	}

	deleteManyCalled := false
	deleteManyFn := ffiDeleterDeleteMany.withFunc(context.Background(), func(*opendalDeleter, **byte, uint, *opendalCancelToken) *opendalError {
		deleteManyCalled = true
		return nil
	})
	if err := deleteManyFn(newTestDeleterHandle(), []string{"ok.txt", "\xff"}, nil); err == nil {
		t.Fatal("ffiDeleterDeleteMany with invalid UTF-8 succeeded, want error")
	}
	if deleteManyCalled {
		t.Fatal("ffiDeleterDeleteMany invoked native symbol for invalid UTF-8 path")
	}
}

func TestListWithStartAfterPropagatesInvalidStringError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		startAfter string
	}{
		{"nul", "a\x00b"},
		{"invalid_utf8", "\xff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cOpts := &opendalListOptions{}
			freed := 0
			listCalled := false
			setStartAfterCalled := false

			ctx := context.Background()
			ctx = context.WithValue(ctx, ffiListOptionsNew.sym, func() *opendalListOptions { return cOpts })
			ctx = context.WithValue(ctx, ffiListOptionsFree.sym, func(opts *opendalListOptions) {
				if opts != cOpts {
					t.Fatalf("freed list options = %p, want %p", opts, cOpts)
				}
				freed++
			})
			ctx = context.WithValue(ctx, ffiListOptionsSetRecursive.sym, func(*opendalListOptions, bool) {})
			ctx = context.WithValue(ctx, ffiListOptionsSetVersions.sym, func(*opendalListOptions, bool) {})
			ctx = context.WithValue(ctx, ffiListOptionsSetDeleted.sym, func(*opendalListOptions, bool) {})
			ctx = context.WithValue(ctx, ffiListOptionsSetStartAfter.sym, ffiListOptionsSetStartAfter.withFunc(ctx, func(*opendalListOptions, *byte) {
				setStartAfterCalled = true
			}))
			ctx = context.WithValue(ctx, ffiOperatorListWith.sym, func(*opendalOperator, string, *opendalListOptions, *opendalCancelToken) (*opendalLister, error) {
				listCalled = true
				return nil, nil
			})

			op := &Operator{ctx: ctx, inner: newTestOperatorHandle()}
			lister, err := op.List(context.Background(), "dir/", ListWithStartAfter(tc.startAfter))
			if err == nil || lister != nil {
				t.Fatalf("List(ListWithStartAfter(%q)) = (%v, %v), want nil and error", tc.startAfter, lister, err)
			}
			if setStartAfterCalled {
				t.Fatal("opendal_list_options_set_start_after was called for invalid string")
			}
			if listCalled {
				t.Fatal("opendal_operator_list_with_cancel was called after startAfter validation failed")
			}
			if freed != 1 {
				t.Fatalf("list options freed %d times, want 1", freed)
			}
		})
	}
}

func TestPresignExpireValidation(t *testing.T) {
	// Empty FFI context: any invalid expire that crosses into FFI panics.
	op := &Operator{ctx: context.Background(), inner: newTestOperatorHandle()}
	bg := context.Background()

	invalidExpires := []time.Duration{
		-time.Hour,
		-time.Second,
		-time.Nanosecond,
		time.Nanosecond,
		500 * time.Millisecond,
		time.Second - time.Nanosecond,
	}
	for _, expire := range invalidExpires {
		for name, call := range map[string]func() (*http.Request, error){
			"PresignRead":   func() (*http.Request, error) { return op.PresignRead(bg, "file.txt", expire) },
			"PresignWrite":  func() (*http.Request, error) { return op.PresignWrite(bg, "file.txt", expire) },
			"PresignDelete": func() (*http.Request, error) { return op.PresignDelete(bg, "file.txt", expire) },
			"PresignStat":   func() (*http.Request, error) { return op.PresignStat(bg, "file.txt", expire) },
		} {
			if req, err := call(); err == nil || req != nil {
				t.Fatalf("%s(%v) = (%v, %v), want nil and error", name, expire, req, err)
			}
		}
	}

	validCases := []struct {
		expire time.Duration
		want   uint64
	}{
		{0, 0},
		{time.Second, 1},
		{1500 * time.Millisecond, 1},
		{time.Hour, 3600},
	}
	for _, tc := range validCases {
		ctx, reqInner, _ := presignTestContext(t, "GET", "https://example.com/valid")
		opInner := newTestOperatorHandle()
		notCalled := uint64(math.MaxUint64)
		gotRead, gotWrite, gotDelete, gotStat := notCalled, notCalled, notCalled, notCalled
		ctx = context.WithValue(ctx, ffiOperatorPresignRead.sym, func(_ *opendalOperator, _ string, expire uint64, _ *opendalReadOptions, _ *opendalCancelToken) (*opendalPresignedRequest, error) {
			gotRead = expire
			return reqInner, nil
		})
		ctx = context.WithValue(ctx, ffiOperatorPresignWrite.sym, func(_ *opendalOperator, _ string, expire uint64, _ *opendalWriteOptions, _ *opendalCancelToken) (*opendalPresignedRequest, error) {
			gotWrite = expire
			return reqInner, nil
		})
		ctx = context.WithValue(ctx, ffiOperatorPresignDelete.sym, func(_ *opendalOperator, _ string, expire uint64, _ *opendalDeleteOptions, _ *opendalCancelToken) (*opendalPresignedRequest, error) {
			gotDelete = expire
			return reqInner, nil
		})
		ctx = context.WithValue(ctx, ffiOperatorPresignStat.sym, func(_ *opendalOperator, _ string, expire uint64, _ *opendalStatOptions, _ *opendalCancelToken) (*opendalPresignedRequest, error) {
			gotStat = expire
			return reqInner, nil
		})
		validOp := &Operator{ctx: ctx, inner: opInner}

		if _, err := validOp.PresignRead(bg, "file.txt", tc.expire); err != nil {
			t.Fatalf("PresignRead(%v): %v", tc.expire, err)
		}
		if _, err := validOp.PresignWrite(bg, "file.txt", tc.expire); err != nil {
			t.Fatalf("PresignWrite(%v): %v", tc.expire, err)
		}
		if _, err := validOp.PresignDelete(bg, "file.txt", tc.expire); err != nil {
			t.Fatalf("PresignDelete(%v): %v", tc.expire, err)
		}
		if _, err := validOp.PresignStat(bg, "file.txt", tc.expire); err != nil {
			t.Fatalf("PresignStat(%v): %v", tc.expire, err)
		}
		if gotRead != tc.want || gotWrite != tc.want || gotDelete != tc.want || gotStat != tc.want {
			t.Fatalf("expire=%v passed seconds (%d, %d, %d, %d), want all %d", tc.expire, gotRead, gotWrite, gotDelete, gotStat, tc.want)
		}
	}
}

func TestRetryFactorValidation(t *testing.T) {
	invalid := []float64{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
		0,
		0.5,
		-2,
		math.Nextafter(math.MaxFloat32, math.Inf(1)),
		1e39,
		math.MaxFloat64,
	}
	for _, factor := range invalid {
		cfg := &operatorConfig{}
		if err := WithRetry(RetryFactor(factor)).apply(cfg); err == nil {
			t.Fatalf("WithRetry(RetryFactor(%v)) succeeded, want error", factor)
		}
	}

	valid := []float64{1, 2.5, math.MaxFloat32}
	for _, factor := range valid {
		cfg := &operatorConfig{}
		if err := WithRetry(RetryFactor(factor)).apply(cfg); err != nil {
			t.Fatalf("WithRetry(RetryFactor(%v)) failed: %v", factor, err)
		}
		if len(cfg.layers) != 1 {
			t.Fatalf("expected 1 retry layer, got %d", len(cfg.layers))
		}
		rl := cfg.layers[0].(retryLayer)
		if math.IsInf(float64(rl.factor), 0) || math.IsNaN(float64(rl.factor)) {
			t.Fatalf("RetryFactor(%v) produced non-finite float32 %v", factor, rl.factor)
		}
		if rl.factor != float32(factor) {
			t.Fatalf("RetryFactor(%v) = %v, want %v", factor, rl.factor, float32(factor))
		}
	}
}

func TestNewMetadataLastModifiedAlwaysUTC(t *testing.T) {
	origLocal := time.Local
	time.Local = time.FixedZone("CST", 8*3600)
	defer func() { time.Local = origLocal }()

	metadataInner := newTestMetadataHandle()
	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiMetaCacheControl.sym, func(*opendalMetadata) *string { return nil })
	ctx = context.WithValue(ctx, ffiMetaContentDisposition.sym, func(*opendalMetadata) *string { return nil })
	ctx = context.WithValue(ctx, ffiMetaContentEncoding.sym, func(*opendalMetadata) *string { return nil })
	ctx = context.WithValue(ctx, ffiMetaContentLanguage.sym, func(*opendalMetadata) *string { return nil })
	ctx = context.WithValue(ctx, ffiMetaContentLength.sym, func(*opendalMetadata) uint64 { return 0 })
	ctx = context.WithValue(ctx, ffiMetaContentMD5.sym, func(*opendalMetadata) *string { return nil })
	ctx = context.WithValue(ctx, ffiMetaContentType.sym, func(*opendalMetadata) *string { return nil })
	ctx = context.WithValue(ctx, ffiMetaETag.sym, func(*opendalMetadata) *string { return nil })
	ctx = context.WithValue(ctx, ffiMetaIsCurrent.sym, func(*opendalMetadata) uint8 { return 2 })
	ctx = context.WithValue(ctx, ffiMetaIsDeleted.sym, func(*opendalMetadata) bool { return false })
	ctx = context.WithValue(ctx, ffiMetaIsFile.sym, func(*opendalMetadata) bool { return true })
	ctx = context.WithValue(ctx, ffiMetaIsDir.sym, func(*opendalMetadata) bool { return false })
	ctx = context.WithValue(ctx, ffiMetaLastModified.sym, func(*opendalMetadata) int64 { return 1700000000123 })
	ctx = context.WithValue(ctx, ffiMetaMode.sym, func(*opendalMetadata) EntryMode { return EntryModeFile })
	ctx = context.WithValue(ctx, ffiMetaVersion.sym, func(*opendalMetadata) *string { return nil })
	ctx = context.WithValue(ctx, ffiMetaUserMetadata.sym, func(*opendalMetadata) map[string]string { return nil })
	ctx = context.WithValue(ctx, ffiMetadataFree.sym, func(*opendalMetadata) {})

	meta := newMetadata(ctx, metadataInner)
	if meta.LastModified().UnixMilli() != 1700000000123 {
		t.Fatalf("LastModified().UnixMilli() = %d, want 1700000000123", meta.LastModified().UnixMilli())
	}
	if meta.LastModified().Location() != time.UTC {
		t.Fatalf("LastModified().Location() = %v, want UTC", meta.LastModified().Location())
	}
}

func TestListerNextClearsStaleErrorOnSuccess(t *testing.T) {
	entryHandle := (*opendalEntry)(unsafe.Pointer(new(byte)))

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiListerNext.sym, func(_ *opendalLister, _ *opendalCancelToken) (*opendalEntry, error) {
		return entryHandle, nil
	})
	ctx = context.WithValue(ctx, ffiEntryName.sym, func(*opendalEntry) string { return "a.txt" })
	ctx = context.WithValue(ctx, ffiEntryPath.sym, func(*opendalEntry) string { return "dir/a.txt" })
	ctx = context.WithValue(ctx, ffiEntryMetadata.sym, func(*opendalEntry) *opendalMetadata { return nil })
	ctx = context.WithValue(ctx, ffiEntryFree.sym, func(*opendalEntry) {})

	lister := &Lister{inner: newTestListerHandle(), ctx: ctx}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if lister.Next(canceledCtx) {
		t.Fatal("Next(canceledCtx) = true, want false")
	}
	var opErr *Error
	if !errors.As(lister.Error(), &opErr) || opErr.Code() != CodeCancelled {
		t.Fatalf("Error() after canceled Next = %v, want CodeCancelled", lister.Error())
	}

	if !lister.Next(context.Background()) {
		t.Fatalf("Next(bg) = false (err=%v), want true", lister.Error())
	}
	if lister.Error() != nil {
		t.Fatalf("Error() after successful Next = %v, want nil", lister.Error())
	}
	if lister.Entry() == nil || lister.Entry().Path() != "dir/a.txt" {
		t.Fatalf("Entry() = %v, want dir/a.txt", lister.Entry())
	}
}

func TestCopyWithSourceContentLengthHintZero(t *testing.T) {
	run := func(opts ...WithCopyFn) (called bool, got uint64) {
		cOpts := &opendalCopyOptions{}
		ctx := context.Background()
		ctx = context.WithValue(ctx, ffiCopyOptionsNew.sym, func() *opendalCopyOptions { return cOpts })
		ctx = context.WithValue(ctx, ffiCopyOptionsFree.sym, func(*opendalCopyOptions) {})
		ctx = context.WithValue(ctx, ffiCopyOptionsSetIfNotExists.sym, func(*opendalCopyOptions, bool) {})
		ctx = context.WithValue(ctx, ffiCopyOptionsSetIfMatch.sym, func(*opendalCopyOptions, string) ([]byte, error) { return nil, nil })
		ctx = context.WithValue(ctx, ffiCopyOptionsSetSourceVersion.sym, func(*opendalCopyOptions, string) ([]byte, error) { return nil, nil })
		ctx = context.WithValue(ctx, ffiCopyOptionsSetSourceContentLengthHint.sym, func(opts *opendalCopyOptions, hint uint64) {
			if opts != cOpts {
				t.Fatalf("copy options = %p, want %p", opts, cOpts)
			}
			called = true
			got = hint
		})

		parsed := parseCopyOptions(opts...)
		built, _, err := newOpendalCopyOptions(ctx, parsed)
		if err != nil {
			t.Fatalf("newOpendalCopyOptions failed: %v", err)
		}
		ffiCopyOptionsFree.symbol(ctx)(built)
		return called, got
	}

	if called, _ := run(); called {
		t.Fatal("sourceContentLengthHint setter called when option was omitted")
	}
	if called, got := run(CopyWithSourceContentLengthHint(0)); !called || got != 0 {
		t.Fatalf("CopyWithSourceContentLengthHint(0): called=%v got=%d, want true, 0", called, got)
	}
	if called, got := run(CopyWithSourceContentLengthHint(42)); !called || got != 42 {
		t.Fatalf("CopyWithSourceContentLengthHint(42): called=%v got=%d, want true, 42", called, got)
	}
}

func TestFunctionalOptionsIgnoreNil(t *testing.T) {
	statOpts := parseStatOptions(nil, StatWithVersion("v1"), nil)
	if statOpts.version != "v1" {
		t.Fatalf("parseStatOptions version = %q, want v1", statOpts.version)
	}

	deleteOpts := parseDeleteOptions(nil, DeleteWithRecursive(true), nil)
	if !deleteOpts.recursive {
		t.Fatal("parseDeleteOptions recursive = false, want true")
	}

	copyOpts := parseCopyOptions(nil, CopyWithIfNotExists(true), nil)
	if !copyOpts.ifNotExists {
		t.Fatal("parseCopyOptions ifNotExists = false, want true")
	}

	writeOpts := parseWriteOptions(nil, WriteWithContentType("text/plain"), nil)
	if writeOpts.contentType != "text/plain" {
		t.Fatalf("parseWriteOptions contentType = %q, want text/plain", writeOpts.contentType)
	}

	readOpts := parseReadOptions(nil, ReadWithVersion("v2"), nil)
	if readOpts.version != "v2" {
		t.Fatalf("parseReadOptions version = %q, want v2", readOpts.version)
	}

	readerOpts := parseReaderOptions(nil, ReaderWithVersion("v3"), nil)
	if readerOpts.version != "v3" {
		t.Fatalf("parseReaderOptions version = %q, want v3", readerOpts.version)
	}

	// Operator.List applies functional options inline rather than via a parse* helper.
	cOpts := &opendalListOptions{}
	var gotRecursive bool
	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiListOptionsNew.sym, func() *opendalListOptions { return cOpts })
	ctx = context.WithValue(ctx, ffiListOptionsFree.sym, func(*opendalListOptions) {})
	ctx = context.WithValue(ctx, ffiListOptionsSetRecursive.sym, func(_ *opendalListOptions, recursive bool) {
		gotRecursive = recursive
	})
	ctx = context.WithValue(ctx, ffiListOptionsSetVersions.sym, func(*opendalListOptions, bool) {})
	ctx = context.WithValue(ctx, ffiListOptionsSetDeleted.sym, func(*opendalListOptions, bool) {})
	ctx = context.WithValue(ctx, ffiOperatorListWith.sym, func(*opendalOperator, string, *opendalListOptions, *opendalCancelToken) (*opendalLister, error) {
		return newTestListerHandle(), nil
	})
	ctx = context.WithValue(ctx, ffiListerFree.sym, func(*opendalLister) {})

	op := &Operator{ctx: ctx, inner: newTestOperatorHandle()}
	lister, err := op.List(context.Background(), "dir/", nil, ListWithRecursive(true), nil)
	if err != nil {
		t.Fatalf("List with nil options failed: %v", err)
	}
	_ = lister.Close()
	if !gotRecursive {
		t.Fatal("List with nil options dropped ListWithRecursive(true)")
	}
}
