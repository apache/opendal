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
	"io"
	"sync"
	"testing"
	"unsafe"
)

// The opendal* handle types are zero-sized, so every &opendalX{} shares
// runtime.zerobase and pointer-identity assertions on them hold no matter which
// handle the code passed. Backing each handle with a byte gives it its own
// address, like newTestDeleterHandle does for the deleter.
func newTestOperatorHandle() *opendalOperator {
	return (*opendalOperator)(unsafe.Pointer(new(byte)))
}

func newTestReaderHandle() *opendalReader {
	return (*opendalReader)(unsafe.Pointer(new(byte)))
}

func newTestWriterHandle() *opendalWriter {
	return (*opendalWriter)(unsafe.Pointer(new(byte)))
}

func newTestListerHandle() *opendalLister {
	return (*opendalLister)(unsafe.Pointer(new(byte)))
}

func newTestCopierHandle() *opendalCopier {
	return (*opendalCopier)(unsafe.Pointer(new(byte)))
}

func newTestMetadataHandle() *opendalMetadata {
	return (*opendalMetadata)(unsafe.Pointer(new(byte)))
}

func TestOperatorCloseFreesOnceAndBlocksLaterOperations(t *testing.T) {
	opInner := newTestOperatorHandle()
	freed := 0
	checks := 0

	ctx := context.WithValue(context.Background(), ffiOperatorFree.sym, func(op *opendalOperator) {
		if op != opInner {
			t.Fatalf("freed operator = %p, want %p", op, opInner)
		}
		freed++
	})
	ctx = context.WithValue(ctx, ffiOperatorCheck.sym, func(op *opendalOperator) error {
		if op != opInner {
			t.Fatalf("checked operator = %p, want %p", op, opInner)
		}
		checks++
		return nil
	})

	cancelled := 0
	op := &Operator{ctx: ctx, inner: opInner, cancel: func() { cancelled++ }}
	op.Close()
	// The second Close is the deferred-call pattern: it must not touch the
	// native library again.
	op.Close()
	if freed != 1 {
		t.Fatalf("operator freed %d times, want 1", freed)
	}
	if cancelled != 1 {
		t.Fatalf("cancel called %d times, want 1", cancelled)
	}
	if err := op.Check(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Check after Close = %v, want ErrOperatorClosed", err)
	}
	if _, err := op.Read("file.txt"); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Read after Close = %v, want ErrOperatorClosed", err)
	}
	if info := op.Info(); info != nil {
		t.Fatalf("Info after Close = %v, want nil", info)
	}
	if checks != 0 {
		t.Fatalf("Check crossed into FFI %d times after Close, want 0", checks)
	}
}

func TestOperatorCloseSerializesWithInFlightCall(t *testing.T) {
	opInner := newTestOperatorHandle()
	inFlight := make(chan struct{})
	release := make(chan struct{})
	freed := 0

	ctx := context.WithValue(context.Background(), ffiOperatorCheck.sym, func(op *opendalOperator) error {
		close(inFlight)
		<-release
		return nil
	})
	ctx = context.WithValue(ctx, ffiOperatorFree.sym, func(*opendalOperator) { freed++ })

	op := &Operator{ctx: ctx, inner: opInner, cancel: func() {}}
	done := make(chan error, 1)
	go func() { done <- op.Check() }()
	<-inFlight
	// Close must wait for the in-flight Check before freeing the handle.
	go op.Close()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Check failed: %v", err)
	}
	// The second, deferred-style Close is a no-op.
	op.Close()
	if freed != 1 {
		t.Fatalf("operator freed %d times, want 1", freed)
	}
}

func TestReaderCloseFreesOnceAndBlocksReuse(t *testing.T) {
	rInner := newTestReaderHandle()
	freed := 0
	reads := 0

	ctx := context.WithValue(context.Background(), ffiReaderFree.sym, func(r *opendalReader) {
		if r != rInner {
			t.Fatalf("freed reader = %p, want %p", r, rInner)
		}
		freed++
	})
	ctx = context.WithValue(ctx, ffiReaderRead.sym, func(r *opendalReader, buf []byte) (uint, error) {
		reads++
		return 0, nil
	})
	ctx = context.WithValue(ctx, ffiReaderSeek.sym, func(r *opendalReader, offset int64, whence int) (int64, error) {
		reads++
		return 0, nil
	})

	r := &Reader{inner: rInner, ctx: ctx}
	if err := r.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if freed != 1 {
		t.Fatalf("reader freed %d times, want 1", freed)
	}
	if _, err := r.Read(make([]byte, 8)); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("Read after Close = %v, want ErrReaderClosed", err)
	}
	if _, err := r.Seek(0, io.SeekStart); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("Seek after Close = %v, want ErrReaderClosed", err)
	}
	if reads != 0 {
		t.Fatalf("read or seek crossed into FFI %d times after Close, want 0", reads)
	}
}

func TestListerCloseFreesOnceAndStopsIteration(t *testing.T) {
	lInner := newTestListerHandle()
	freed := 0
	nexts := 0

	ctx := context.WithValue(context.Background(), ffiListerFree.sym, func(l *opendalLister) {
		if l != lInner {
			t.Fatalf("freed lister = %p, want %p", l, lInner)
		}
		freed++
	})
	ctx = context.WithValue(ctx, ffiListerNext.sym, ffiListerNext.withFunc(ctx, func(l *opendalLister, entry **opendalEntry) *opendalError {
		nexts++
		return nil
	}))

	l := &Lister{inner: lInner, ctx: ctx}
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if freed != 1 {
		t.Fatalf("lister freed %d times, want 1", freed)
	}
	if l.Next() {
		t.Fatal("Next after Close = true, want false")
	}
	if err := l.Error(); !errors.Is(err, ErrListerClosed) {
		t.Fatalf("Error after Close = %v, want ErrListerClosed", err)
	}
	if nexts != 0 {
		t.Fatalf("Next crossed into FFI %d times after Close, want 0", nexts)
	}
}

func TestWriterCloseClosesAndFreesOnce(t *testing.T) {
	wInner := newTestWriterHandle()
	meta := newTestMetadataHandle()
	closed := 0
	freed := 0
	writes := 0

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiWriterCloseWithMetadata.sym, ffiWriterCloseWithMetadata.withFunc(ctx, func(w *opendalWriter, out **opendalMetadata) *opendalError {
		if w != wInner {
			t.Fatalf("closed writer = %p, want %p", w, wInner)
		}
		closed++
		*out = meta
		return nil
	}))
	ctx = context.WithValue(ctx, ffiWriterFree.sym, func(w *opendalWriter) {
		if w != wInner {
			t.Fatalf("freed writer = %p, want %p", w, wInner)
		}
		freed++
	})
	ctx = context.WithValue(ctx, ffiWriterWrite.sym, func(w *opendalWriter, buf []byte) (int, error) {
		writes++
		return len(buf), nil
	})
	ctx = minimalMetadataContext(ctx, t, meta)

	w := &Writer{inner: wInner, ctx: ctx}
	got, err := w.Close()
	if err != nil || got == nil {
		t.Fatalf("Close = (%v, %v), want metadata and nil error", got, err)
	}
	if _, err := w.Close(); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("second Close = %v, want ErrWriterClosed", err)
	}
	if closed != 1 {
		t.Fatalf("writer closed natively %d times, want 1", closed)
	}
	if freed != 1 {
		t.Fatalf("writer freed %d times, want 1", freed)
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Write after Close = %v, want ErrWriterClosed", err)
	}
	if writes != 0 {
		t.Fatalf("Write crossed into FFI %d times after Close, want 0", writes)
	}
}

func TestWriterCloseReleasesAndReportsFailedNativeClose(t *testing.T) {
	wInner := newTestWriterHandle()
	closed := 0
	freed := 0

	// ffiErrorFree must precede the withFunc wrapper: the wrapper captures the
	// context it is built with for parseError.
	ctx := context.WithValue(context.Background(), ffiErrorFree.sym, func(err *opendalError) {})
	ctx = context.WithValue(ctx, ffiWriterCloseWithMetadata.sym, ffiWriterCloseWithMetadata.withFunc(ctx, func(w *opendalWriter, out **opendalMetadata) *opendalError {
		closed++
		return &opendalError{code: int32(CodeUnexpected)}
	}))
	ctx = context.WithValue(ctx, ffiWriterFree.sym, func(w *opendalWriter) { freed++ })

	w := &Writer{inner: wInner, ctx: ctx}
	got, err := w.Close()
	var opErr *Error
	if got != nil || !errors.As(err, &opErr) || opErr.Code() != CodeUnexpected {
		t.Fatalf("Close = (%v, %v), want nil and CodeUnexpected", got, err)
	}
	if closed != 1 || freed != 1 {
		t.Fatalf("native close ran %d times and free ran %d times, want 1 and 1", closed, freed)
	}
	if _, err := w.Close(); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("second Close after failed native close = %v, want ErrWriterClosed", err)
	}
	if closed != 1 || freed != 1 {
		t.Fatalf("after second Close: native close ran %d times and free ran %d times, want 1 and 1", closed, freed)
	}
}

func TestClosesRaceWithEachOther(t *testing.T) {
	// Close from a defer plus an explicit call, or two deferred closes through
	// helper wrappers, can run on different goroutines. Each native handle must
	// still be freed exactly once.
	freed := 0
	var freeMu sync.Mutex
	count := func() { freeMu.Lock(); freed++; freeMu.Unlock() }

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiReaderFree.sym, func(*opendalReader) { count() })
	ctx = context.WithValue(ctx, ffiListerFree.sym, func(*opendalLister) { count() })
	ctx = context.WithValue(ctx, ffiCopierFree.sym, func(*opendalCopier) { count() })
	ctx = context.WithValue(ctx, ffiWriterFree.sym, func(*opendalWriter) { count() })
	ctx = context.WithValue(ctx, ffiErrorFree.sym, func(err *opendalError) {})
	ctx = context.WithValue(ctx, ffiWriterCloseWithMetadata.sym, ffiWriterCloseWithMetadata.withFunc(ctx, func(w *opendalWriter, out **opendalMetadata) *opendalError {
		return &opendalError{code: int32(CodeUnexpected)}
	}))
	ctx = context.WithValue(ctx, ffiDeleterFree.sym, func(*opendalDeleter) { count() })
	ctx = context.WithValue(ctx, ffiDeleterFlush.sym, func(*opendalDeleter) error { return nil })
	ctx = context.WithValue(ctx, ffiOperatorFree.sym, func(*opendalOperator) { count() })

	reader := &Reader{inner: newTestReaderHandle(), ctx: ctx}
	lister := &Lister{inner: newTestListerHandle(), ctx: ctx}
	copier := &Copier{inner: newTestCopierHandle(), ctx: ctx}
	deleter := &Deleter{inner: newTestDeleterHandle(), ctx: ctx}
	writer := &Writer{inner: newTestWriterHandle(), ctx: ctx}
	operator := &Operator{inner: newTestOperatorHandle(), ctx: ctx, cancel: func() {}}
	closers := []func(){
		func() { _ = reader.Close() },
		func() { _ = lister.Close() },
		func() { _ = copier.Close() },
		func() { _ = deleter.Close() },
		func() { _, _ = writer.Close() },
		operator.Close,
	}

	var wg sync.WaitGroup
	for _, close := range closers {
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				close()
			}()
		}
	}
	wg.Wait()
	if freed != len(closers) {
		t.Fatalf("native handles freed %d times under concurrent Close, want %d", freed, len(closers))
	}
}

// minimalMetadataContext injects the symbols newMetadata needs with empty
// values so tests can exercise the Writer.Close success path without a native
// library.
func minimalMetadataContext(ctx context.Context, t *testing.T, want *opendalMetadata) context.Context {
	t.Helper()

	check := func(m *opendalMetadata) {
		t.Helper()
		if m != want {
			t.Fatalf("metadata = %p, want %p", m, want)
		}
	}

	ctx = context.WithValue(ctx, ffiMetadataFree.sym, func(m *opendalMetadata) {
		check(m)
	})
	ctx = context.WithValue(ctx, ffiMetaLastModified.sym, func(m *opendalMetadata) int64 {
		check(m)
		return -1
	})
	ctx = context.WithValue(ctx, ffiMetaIsCurrent.sym, func(m *opendalMetadata) uint8 {
		check(m)
		return 0
	})
	ctx = context.WithValue(ctx, ffiMetaCacheControl.sym, func(m *opendalMetadata) *string {
		check(m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentDisposition.sym, func(m *opendalMetadata) *string {
		check(m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentEncoding.sym, func(m *opendalMetadata) *string {
		check(m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentLength.sym, func(m *opendalMetadata) uint64 {
		check(m)
		return 0
	})
	ctx = context.WithValue(ctx, ffiMetaContentMD5.sym, func(m *opendalMetadata) *string {
		check(m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaContentType.sym, func(m *opendalMetadata) *string {
		check(m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaEtag.sym, func(m *opendalMetadata) *string {
		check(m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaIsDeleted.sym, func(m *opendalMetadata) bool {
		check(m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsFile.sym, func(m *opendalMetadata) bool {
		check(m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaIsDir.sym, func(m *opendalMetadata) bool {
		check(m)
		return false
	})
	ctx = context.WithValue(ctx, ffiMetaMode.sym, func(m *opendalMetadata) EntryMode {
		check(m)
		return EntryModeUnknown
	})
	ctx = context.WithValue(ctx, ffiMetaVersion.sym, func(m *opendalMetadata) *string {
		check(m)
		return nil
	})
	ctx = context.WithValue(ctx, ffiMetaUserMetadata.sym, func(m *opendalMetadata) map[string]string {
		check(m)
		return nil
	})
	return ctx
}
