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
	"strings"
	"sync"
	"testing"
	"time"
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
	ctx = context.WithValue(ctx, ffiOperatorCheck.sym, func(op *opendalOperator, _ *opendalCancelToken) error {
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
	if err := op.Check(context.Background()); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Check after Close = %v, want ErrOperatorClosed", err)
	}
	if _, err := op.Read(context.Background(), "file.txt"); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Read after Close = %v, want ErrOperatorClosed", err)
	}
	if info := op.Info(); info != nil {
		t.Fatalf("Info after Close = %v, want nil", info)
	}
	if checks != 0 {
		t.Fatalf("Check crossed into FFI %d times after Close, want 0", checks)
	}
}

// TestOperatorMethodsFailCleanlyAfterClose covers every Operator method that
// reads op.ctx or op.inner: on a closed, zero-value, or nil Operator each must
// return ErrOperatorClosed (or nil for Info) without resolving a single FFI
// symbol. The context carries no symbols, so any method that regresses and
// touches the native library panics instead of passing silently.
func TestOperatorMethodsFailCleanlyAfterClose(t *testing.T) {
	ctx := context.WithValue(context.Background(), ffiOperatorFree.sym, func(*opendalOperator) {})
	closed := &Operator{ctx: ctx, inner: newTestOperatorHandle(), cancel: func() {}}
	closed.Close()

	operators := map[string]*Operator{
		"closed":     closed,
		"zero-value": {},
		"nil":        nil,
	}
	bg := context.Background()
	for state, op := range operators {
		t.Run(state, func(t *testing.T) {
			checks := map[string]func(*Operator) error{
				"Check":     func(op *Operator) error { return op.Check(bg) },
				"CreateDir": func(op *Operator) error { return op.CreateDir(bg, "dir/") },
				"IsExist":   func(op *Operator) error { _, err := op.IsExist(bg, "file.txt"); return err },
				"Rename":    func(op *Operator) error { return op.Rename(bg, "a.txt", "b.txt") },
				"Remove":    func(op *Operator) error { return op.Remove(bg, []string{"a.txt"}) },
				"Read":      func(op *Operator) error { _, err := op.Read(bg, "file.txt"); return err },
				"ReadWithOpts": func(op *Operator) error {
					_, err := op.Read(bg, "file.txt", ReadWithRange(0, 1))
					return err
				},
				"Write": func(op *Operator) error { _, err := op.Write(bg, "file.txt", []byte("x")); return err },
				"WriteWithOpts": func(op *Operator) error {
					_, err := op.Write(bg, "file.txt", []byte("x"), WriteWithContentType("text/plain"))
					return err
				},
				"Stat":         func(op *Operator) error { _, err := op.Stat(bg, "file.txt"); return err },
				"StatWithOpts": func(op *Operator) error { _, err := op.Stat(bg, "file.txt", StatWithVersion("v")); return err },
				"Delete":       func(op *Operator) error { return op.Delete(bg, "file.txt") },
				"DeleteWithOpts": func(op *Operator) error {
					return op.Delete(bg, "file.txt", DeleteWithVersion("v"))
				},
				"Copy":         func(op *Operator) error { return op.Copy(bg, "a.txt", "b.txt") },
				"CopyWithOpts": func(op *Operator) error { return op.Copy(bg, "a.txt", "b.txt", CopyWithIfNotExists(true)) },
				"List":         func(op *Operator) error { _, err := op.List(bg, "dir/"); return err },
				"ListWithOpts": func(op *Operator) error { _, err := op.List(bg, "dir/", ListWithRecursive(true)); return err },
				"Reader":       func(op *Operator) error { _, err := op.Reader(bg, "file.txt"); return err },
				"ReaderWithOpts": func(op *Operator) error {
					_, err := op.Reader(bg, "file.txt", ReaderWithVersion("v"))
					return err
				},
				"Writer": func(op *Operator) error { _, err := op.Writer(bg, "file.txt"); return err },
				"WriterWithOpts": func(op *Operator) error {
					_, err := op.Writer(bg, "file.txt", WriteWithContentType("text/plain"))
					return err
				},
				"Copier":         func(op *Operator) error { _, err := op.Copier(bg, "a.txt", "b.txt"); return err },
				"CopierWithOpts": func(op *Operator) error { _, err := op.Copier(bg, "a.txt", "b.txt", CopyWithChunk(1)); return err },
				"Deleter":        func(op *Operator) error { _, err := op.Deleter(bg); return err },
				"WithDeleter": func(op *Operator) error {
					return op.WithDeleter(bg, func(*Deleter) error { return nil })
				},
				"PresignRead": func(op *Operator) error { _, err := op.PresignRead(bg, "file.txt", time.Minute); return err },
				"PresignReadWithOpts": func(op *Operator) error {
					_, err := op.PresignRead(bg, "file.txt", time.Minute, ReadWithVersion("v"))
					return err
				},
				"PresignWrite": func(op *Operator) error { _, err := op.PresignWrite(bg, "file.txt", time.Minute); return err },
				"PresignWriteWithOpts": func(op *Operator) error {
					_, err := op.PresignWrite(bg, "file.txt", time.Minute, WriteWithContentType("text/plain"))
					return err
				},
				"PresignDelete": func(op *Operator) error { _, err := op.PresignDelete(bg, "file.txt", time.Minute); return err },
				"PresignDeleteWithOpts": func(op *Operator) error {
					_, err := op.PresignDelete(bg, "file.txt", time.Minute, DeleteWithVersion("v"))
					return err
				},
				"PresignStat": func(op *Operator) error { _, err := op.PresignStat(bg, "file.txt", time.Minute); return err },
				"PresignStatWithOpts": func(op *Operator) error {
					_, err := op.PresignStat(bg, "file.txt", time.Minute, StatWithVersion("v"))
					return err
				},
			}
			for name, check := range checks {
				if err := check(op); !errors.Is(err, ErrOperatorClosed) {
					t.Errorf("%s = %v, want ErrOperatorClosed", name, err)
				}
			}
			if info := op.Info(); info != nil {
				t.Errorf("Info = %v, want nil", info)
			}
		})
	}
}

func TestNilHandleReceivers(t *testing.T) {
	bg := context.Background()
	var op *Operator
	op.Close()
	var r *Reader
	if err := r.Close(); err != nil {
		t.Fatalf("nil Reader Close = %v, want nil", err)
	}
	if _, err := r.Read(make([]byte, 4)); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("nil Reader Read = %v, want ErrReaderClosed", err)
	}
	if _, err := r.ReadContext(bg, make([]byte, 4)); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("nil Reader ReadContext = %v, want ErrReaderClosed", err)
	}
	if _, err := r.Seek(0, io.SeekStart); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("nil Reader Seek = %v, want ErrReaderClosed", err)
	}
	if _, err := r.SeekContext(bg, 0, io.SeekStart); !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("nil Reader SeekContext = %v, want ErrReaderClosed", err)
	}
	var l *Lister
	if err := l.Close(); err != nil {
		t.Fatalf("nil Lister Close = %v, want nil", err)
	}
	if l.Next(bg) {
		t.Fatal("nil Lister Next = true, want false")
	}
	if err := l.Error(); !errors.Is(err, ErrListerClosed) {
		t.Fatalf("nil Lister Error = %v, want ErrListerClosed", err)
	}
	if entry := l.Entry(); entry != nil {
		t.Fatalf("nil Lister Entry = %v, want nil", entry)
	}
	var w *Writer
	if _, err := w.Close(bg); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("nil Writer Close = %v, want ErrWriterClosed", err)
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("nil Writer Write = %v, want ErrWriterClosed", err)
	}
	if _, err := w.WriteContext(bg, []byte("x")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("nil Writer WriteContext = %v, want ErrWriterClosed", err)
	}
	if _, err := w.ReadFrom(strings.NewReader("x")); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("nil Writer ReadFrom = %v, want ErrWriterClosed", err)
	}
	var d *Deleter
	if err := d.Close(bg); err != nil {
		t.Fatalf("nil Deleter Close = %v, want nil", err)
	}
	d.Discard()
	if err := d.Delete(bg, "a.txt"); !errors.Is(err, ErrDeleterClosed) {
		t.Fatalf("nil Deleter Delete = %v, want ErrDeleterClosed", err)
	}
	if err := d.Flush(bg); !errors.Is(err, ErrDeleterClosed) {
		t.Fatalf("nil Deleter Flush = %v, want ErrDeleterClosed", err)
	}
	var c *Copier
	if err := c.Close(); err != nil {
		t.Fatalf("nil Copier Close = %v, want nil", err)
	}
	if _, ok, err := c.Next(bg); ok || err != nil {
		t.Fatalf("nil Copier Next = (_, %v, %v), want (_, false, nil)", ok, err)
	}
	if err := c.Abort(bg); err != nil {
		t.Fatalf("nil Copier Abort = %v, want nil", err)
	}
}

func TestReaderReadAfterCloseRejectsEmptyBuffer(t *testing.T) {
	ctx := context.WithValue(context.Background(), ffiReaderFree.sym, func(*opendalReader) {})

	r := &Reader{inner: newTestReaderHandle(), ctx: ctx}
	if err := r.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	// A zero-length read must report the closed state too, so it behaves like
	// Writer.Write on a closed Writer.
	if n, err := r.Read(nil); n != 0 || !errors.Is(err, ErrReaderClosed) {
		t.Fatalf("Read(nil) after Close = (%d, %v), want (0, ErrReaderClosed)", n, err)
	}
}

func TestWriterReadFromAfterCloseReadsNothing(t *testing.T) {
	wInner := newTestWriterHandle()
	freed := 0

	ctx := context.WithValue(context.Background(), ffiWriterFree.sym, func(*opendalWriter) { freed++ })
	ctx = context.WithValue(ctx, ffiErrorFree.sym, func(err *opendalError) {})
	ctx = context.WithValue(ctx, ffiWriterCloseWithMetadata.sym, ffiWriterCloseWithMetadata.withFunc(ctx, func(w *opendalWriter, _ *opendalCancelToken, out **opendalMetadata) *opendalError {
		return &opendalError{code: int32(CodeUnexpected)}
	}))
	ctx = context.WithValue(ctx, ffiWriterWrite.sym, func(w *opendalWriter, buf []byte, _ *opendalCancelToken) (int, error) {
		t.Fatal("Write crossed into FFI on a closed writer")
		return 0, nil
	})

	w := &Writer{inner: wInner, ctx: ctx}
	if _, err := w.Close(context.Background()); err == nil {
		t.Fatal("Close succeeded, want the stubbed failure")
	}
	if freed != 1 {
		t.Fatalf("writer freed %d times, want 1", freed)
	}
	if n, err := w.ReadFrom(strings.NewReader("data")); n != 0 || !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("ReadFrom after Close = (%d, %v), want (0, ErrWriterClosed)", n, err)
	}
}

func TestCopierStaysQuietAfterClose(t *testing.T) {
	cInner := newTestCopierHandle()
	calls := 0

	ctx := context.WithValue(context.Background(), ffiCopierFree.sym, func(c *opendalCopier) {
		calls++
	})
	ctx = context.WithValue(ctx, ffiCopierNext.sym, ffiCopierNext.withFunc(ctx, func(c *opendalCopier, _ *opendalCancelToken, size *uint, hasNext *bool) *opendalError {
		t.Fatal("Next crossed into FFI on a closed copier")
		return nil
	}))
	ctx = context.WithValue(ctx, ffiCopierAbort.sym, ffiCopierAbort.withFunc(ctx, func(c *opendalCopier, _ *opendalCancelToken) *opendalError {
		t.Fatal("Abort crossed into FFI on a closed copier")
		return nil
	}))

	c := &Copier{inner: cInner, ctx: ctx}
	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("copier freed %d times, want 1", calls)
	}
	if size, ok, err := c.Next(context.Background()); size != 0 || ok || err != nil {
		t.Fatalf("Next after Close = (%d, %v, %v), want (0, false, nil)", size, ok, err)
	}
	if err := c.Abort(context.Background()); err != nil {
		t.Fatalf("Abort after Close = %v, want nil", err)
	}
}

func TestOperatorCloseSerializesWithInFlightCall(t *testing.T) {
	opInner := newTestOperatorHandle()
	inFlight := make(chan struct{})
	release := make(chan struct{})
	freed := 0

	ctx := context.WithValue(context.Background(), ffiOperatorCheck.sym, func(op *opendalOperator, _ *opendalCancelToken) error {
		close(inFlight)
		<-release
		return nil
	})
	ctx = context.WithValue(ctx, ffiOperatorFree.sym, func(*opendalOperator) { freed++ })

	op := &Operator{ctx: ctx, inner: opInner, cancel: func() {}}
	done := make(chan error, 1)
	go func() { done <- op.Check(context.Background()) }()
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
	ctx = context.WithValue(ctx, ffiReaderRead.sym, func(r *opendalReader, buf []byte, _ *opendalCancelToken) (uint, error) {
		reads++
		return 0, nil
	})
	ctx = context.WithValue(ctx, ffiReaderSeek.sym, func(r *opendalReader, offset int64, whence int, _ *opendalCancelToken) (int64, error) {
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
	ctx = context.WithValue(ctx, ffiListerNext.sym, ffiListerNext.withFunc(ctx, func(l *opendalLister, _ *opendalCancelToken, entry **opendalEntry) *opendalError {
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
	if l.Next(context.Background()) {
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
	ctx = context.WithValue(ctx, ffiWriterCloseWithMetadata.sym, ffiWriterCloseWithMetadata.withFunc(ctx, func(w *opendalWriter, _ *opendalCancelToken, out **opendalMetadata) *opendalError {
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
	ctx = context.WithValue(ctx, ffiWriterWrite.sym, func(w *opendalWriter, buf []byte, _ *opendalCancelToken) (int, error) {
		writes++
		return len(buf), nil
	})
	ctx = minimalMetadataContext(ctx, t, meta)

	w := &Writer{inner: wInner, ctx: ctx}
	got, err := w.Close(context.Background())
	if err != nil || got == nil {
		t.Fatalf("Close = (%v, %v), want metadata and nil error", got, err)
	}
	if _, err := w.Close(context.Background()); !errors.Is(err, ErrWriterClosed) {
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
	ctx = context.WithValue(ctx, ffiWriterCloseWithMetadata.sym, ffiWriterCloseWithMetadata.withFunc(ctx, func(w *opendalWriter, _ *opendalCancelToken, out **opendalMetadata) *opendalError {
		closed++
		return &opendalError{code: int32(CodeUnexpected)}
	}))
	ctx = context.WithValue(ctx, ffiWriterFree.sym, func(w *opendalWriter) { freed++ })

	w := &Writer{inner: wInner, ctx: ctx}
	got, err := w.Close(context.Background())
	var opErr *Error
	if got != nil || !errors.As(err, &opErr) || opErr.Code() != CodeUnexpected {
		t.Fatalf("Close = (%v, %v), want nil and CodeUnexpected", got, err)
	}
	if closed != 1 || freed != 1 {
		t.Fatalf("native close ran %d times and free ran %d times, want 1 and 1", closed, freed)
	}
	if _, err := w.Close(context.Background()); !errors.Is(err, ErrWriterClosed) {
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
	ctx = context.WithValue(ctx, ffiWriterCloseWithMetadata.sym, ffiWriterCloseWithMetadata.withFunc(ctx, func(w *opendalWriter, _ *opendalCancelToken, out **opendalMetadata) *opendalError {
		return &opendalError{code: int32(CodeUnexpected)}
	}))
	ctx = context.WithValue(ctx, ffiDeleterFree.sym, func(*opendalDeleter) { count() })
	ctx = context.WithValue(ctx, ffiDeleterFlush.sym, func(*opendalDeleter, *opendalCancelToken) error { return nil })
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
		func() { _ = deleter.Close(context.Background()) },
		func() { _, _ = writer.Close(context.Background()) },
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
