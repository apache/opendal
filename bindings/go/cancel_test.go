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
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func newTestCancelTokenHandle() *opendalCancelToken {
	return (*opendalCancelToken)(unsafe.Pointer(new(byte)))
}

func assertCanceledError(t *testing.T, name string, err error, wantTarget error) {
	t.Helper()
	if !errors.Is(err, wantTarget) {
		t.Fatalf("%s error = %v, want errors.Is(_, %v)", name, err, wantTarget)
	}
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Code() != CodeCancelled {
		t.Fatalf("%s error = %v, want *Error with CodeCancelled", name, err)
	}
}

func TestPreCanceledOperatorAndHandleMethodsShortCircuit(t *testing.T) {
	cases := []struct {
		name   string
		newCtx func() (context.Context, context.CancelFunc)
		want   error
	}{
		{
			name: "canceled",
			newCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			want: context.Canceled,
		},
		{
			name: "deadline_exceeded",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			want: context.DeadlineExceeded,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.newCtx()
			defer cancel()

			// Empty FFI context: any method that crosses into FFI on a
			// pre-cancelled context panics instead of passing silently.
			op := &Operator{ctx: context.Background(), inner: newTestOperatorHandle()}

			operatorChecks := map[string]func() error{
				"Check":        func() error { return op.Check(ctx) },
				"CreateDir":    func() error { return op.CreateDir(ctx, "dir/") },
				"IsExist":      func() error { _, err := op.IsExist(ctx, "file.txt"); return err },
				"Rename":       func() error { return op.Rename(ctx, "a.txt", "b.txt") },
				"Remove":       func() error { return op.Remove(ctx, []string{"a.txt"}) },
				"RemoveEmpty":  func() error { return op.Remove(ctx, nil) },
				"Read":         func() error { _, err := op.Read(ctx, "file.txt"); return err },
				"ReadWithOpts": func() error { _, err := op.Read(ctx, "file.txt", ReadWithRange(0, 1)); return err },
				"Write":        func() error { _, err := op.Write(ctx, "file.txt", []byte("x")); return err },
				"WriteWithOpts": func() error {
					_, err := op.Write(ctx, "file.txt", []byte("x"), WriteWithContentType("text/plain"))
					return err
				},
				"Stat":           func() error { _, err := op.Stat(ctx, "file.txt"); return err },
				"StatWithOpts":   func() error { _, err := op.Stat(ctx, "file.txt", StatWithVersion("v")); return err },
				"Delete":         func() error { return op.Delete(ctx, "file.txt") },
				"DeleteWithOpts": func() error { return op.Delete(ctx, "file.txt", DeleteWithVersion("v")) },
				"Copy":           func() error { return op.Copy(ctx, "a.txt", "b.txt") },
				"CopyWithOpts":   func() error { return op.Copy(ctx, "a.txt", "b.txt", CopyWithIfNotExists(true)) },
				"List":           func() error { _, err := op.List(ctx, "dir/"); return err },
				"ListWithOpts":   func() error { _, err := op.List(ctx, "dir/", ListWithRecursive(true)); return err },
				"Reader":         func() error { _, err := op.Reader(ctx, "file.txt"); return err },
				"ReaderWithOpts": func() error { _, err := op.Reader(ctx, "file.txt", ReaderWithVersion("v")); return err },
				"Writer":         func() error { _, err := op.Writer(ctx, "file.txt"); return err },
				"WriterWithOpts": func() error { _, err := op.Writer(ctx, "file.txt", WriteWithContentType("text/plain")); return err },
				"Copier":         func() error { _, err := op.Copier(ctx, "a.txt", "b.txt"); return err },
				"CopierWithOpts": func() error { _, err := op.Copier(ctx, "a.txt", "b.txt", CopyWithChunk(1)); return err },
				"Deleter":        func() error { _, err := op.Deleter(ctx); return err },
				"WithDeleter":    func() error { return op.WithDeleter(ctx, func(*Deleter) error { return nil }) },
				"PresignRead":    func() error { _, err := op.PresignRead(ctx, "file.txt", time.Minute); return err },
				"PresignWrite":   func() error { _, err := op.PresignWrite(ctx, "file.txt", time.Minute); return err },
				"PresignDelete":  func() error { _, err := op.PresignDelete(ctx, "file.txt", time.Minute); return err },
				"PresignStat":    func() error { _, err := op.PresignStat(ctx, "file.txt", time.Minute); return err },
			}
			for name, fn := range operatorChecks {
				assertCanceledError(t, name, fn(), tc.want)
			}

			r := &Reader{inner: newTestReaderHandle(), ctx: context.Background()}
			if n, err := r.ReadContext(ctx, make([]byte, 4)); n != 0 {
				t.Fatalf("Reader.ReadContext n = %d, want 0", n)
			} else {
				assertCanceledError(t, "Reader.ReadContext", err, tc.want)
			}
			if n, err := r.ReadContext(ctx, nil); n != 0 {
				t.Fatalf("Reader.ReadContext(nil) n = %d, want 0", n)
			} else {
				assertCanceledError(t, "Reader.ReadContext(nil)", err, tc.want)
			}
			if pos, err := r.SeekContext(ctx, 0, io.SeekStart); pos != 0 {
				t.Fatalf("Reader.SeekContext pos = %d, want 0", pos)
			} else {
				assertCanceledError(t, "Reader.SeekContext", err, tc.want)
			}

			wFreed := 0
			wCtx := context.WithValue(context.Background(), ffiWriterFree.sym, func(*opendalWriter) { wFreed++ })
			w := &Writer{inner: newTestWriterHandle(), ctx: wCtx}
			if n, err := w.WriteContext(ctx, []byte("x")); n != 0 {
				t.Fatalf("Writer.WriteContext n = %d, want 0", n)
			} else {
				assertCanceledError(t, "Writer.WriteContext", err, tc.want)
			}
			if meta, err := w.Close(ctx); meta != nil {
				t.Fatalf("Writer.Close meta = %v, want nil", meta)
			} else {
				assertCanceledError(t, "Writer.Close", err, tc.want)
			}
			if wFreed != 1 {
				t.Fatalf("Writer.Close on pre-cancelled context freed handle %d times, want 1", wFreed)
			}

			l := &Lister{inner: newTestListerHandle(), ctx: context.Background()}
			if l.Next(ctx) {
				t.Fatal("Lister.Next = true on pre-cancelled context, want false")
			}
			assertCanceledError(t, "Lister.Next", l.Error(), tc.want)

			c := &Copier{inner: newTestCopierHandle(), ctx: context.Background()}
			if size, ok, err := c.Next(ctx); size != 0 || ok {
				t.Fatalf("Copier.Next = (%d, %v, %v), want (0, false, canceled)", size, ok, err)
			} else {
				assertCanceledError(t, "Copier.Next", err, tc.want)
			}
			assertCanceledError(t, "Copier.Abort", c.Abort(ctx), tc.want)

			dFreed := 0
			dCtx := context.WithValue(context.Background(), ffiDeleterFree.sym, func(*opendalDeleter) { dFreed++ })
			d := &Deleter{inner: newTestDeleterHandle(), ctx: dCtx}
			assertCanceledError(t, "Deleter.Delete", d.Delete(ctx, "a.txt"), tc.want)
			assertCanceledError(t, "Deleter.Flush", d.Flush(ctx), tc.want)
			assertCanceledError(t, "Deleter.Close", d.Close(ctx), tc.want)
			if dFreed != 1 {
				t.Fatalf("Deleter.Close on pre-cancelled context freed handle %d times, want 1", dFreed)
			}
		})
	}
}

func TestWithCancelTokenFastPathSkipsTokenAllocation(t *testing.T) {
	// Empty FFI context ensures ffiCancelTokenNew/Cancel/Free are never called
	// for nil or non-cancellable contexts.
	for _, callCtx := range []context.Context{nil, context.Background(), context.TODO()} {
		called := false
		err := withCancelToken(context.Background(), callCtx, func(token *opendalCancelToken) error {
			called = true
			if token != nil {
				t.Fatalf("expected nil cancel token for non-cancellable context, got %p", token)
			}
			return nil
		})
		if err != nil || !called {
			t.Fatalf("withCancelToken = %v (called=%v), want nil (called=true)", err, called)
		}
	}
}

func TestMidFlightCancellationWaitsForAfterFuncBeforeFree(t *testing.T) {
	tok := newTestCancelTokenHandle()
	inFlight := make(chan struct{})
	cancelEntered := make(chan struct{})
	allowCancelReturn := make(chan struct{})
	cancelFinished := atomic.Bool{}
	freed := atomic.Int32{}

	ffiCtx := context.Background()
	ffiCtx = context.WithValue(ffiCtx, ffiCancelTokenNew.sym, func() *opendalCancelToken { return tok })
	ffiCtx = context.WithValue(ffiCtx, ffiCancelTokenCancel.sym, func(got *opendalCancelToken) {
		if got != tok {
			t.Errorf("cancel received %p, want %p", got, tok)
		}
		close(cancelEntered)
		<-allowCancelReturn
		cancelFinished.Store(true)
	})
	ffiCtx = context.WithValue(ffiCtx, ffiCancelTokenFree.sym, func(got *opendalCancelToken) {
		if got != tok {
			t.Errorf("free received %p, want %p", got, tok)
		}
		if !cancelFinished.Load() {
			t.Error("opendal_cancel_token_free ran before opendal_cancel_token_cancel returned")
		}
		freed.Add(1)
	})
	ffiCtx = context.WithValue(ffiCtx, ffiOperatorCheck.sym, func(_ *opendalOperator, got *opendalCancelToken) error {
		if got != tok {
			t.Errorf("check received %p, want %p", got, tok)
		}
		close(inFlight)
		<-cancelEntered
		return &Error{code: CodeCancelled, message: "operation cancelled"}
	})

	op := &Operator{ctx: ffiCtx, inner: newTestOperatorHandle()}
	callCtx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- op.Check(callCtx)
	}()

	<-inFlight
	cancel()
	<-cancelEntered

	// op.Check's FFI call has returned CodeCancelled, but AfterFunc is still
	// inside cancelFn. withCancelToken must block before freeing the token.
	select {
	case err := <-errCh:
		t.Fatalf("op.Check returned early while cancel callback was still running: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(allowCancelReturn)
	err := <-errCh
	assertCanceledError(t, "mid-flight Check", err, context.Canceled)
	if freed.Load() != 1 {
		t.Fatalf("token freed %d times, want 1", freed.Load())
	}

	// Verify mid-flight deadline expiration maps OPENDAL_CANCELLED to context.DeadlineExceeded.
	deadlineHit := make(chan struct{})
	ffiCtx = context.WithValue(ffiCtx, ffiCancelTokenCancel.sym, func(got *opendalCancelToken) {
		close(deadlineHit)
	})
	ffiCtx = context.WithValue(ffiCtx, ffiOperatorCheck.sym, func(_ *opendalOperator, _ *opendalCancelToken) error {
		<-deadlineHit
		return &Error{code: CodeCancelled, message: "operation cancelled"}
	})
	opDeadline := &Operator{ctx: ffiCtx, inner: newTestOperatorHandle()}
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer timeoutCancel()
	assertCanceledError(t, "mid-flight Check(timeout)", opDeadline.Check(timeoutCtx), context.DeadlineExceeded)
	if freed.Load() != 2 {
		t.Fatalf("token freed %d times, want 2", freed.Load())
	}
}

func TestWithCancelTokenRaceStress(t *testing.T) {
	// Exercise concurrent cancellation vs completion across many goroutines
	// to verify under -race that cancelFn never touches a freed token.
	type fakeToken struct {
		mu     sync.Mutex
		freed  bool
		cancel bool
	}

	var mu sync.Mutex
	tokens := map[*opendalCancelToken]*fakeToken{}

	ffiCtx := context.Background()
	ffiCtx = context.WithValue(ffiCtx, ffiCancelTokenNew.sym, func() *opendalCancelToken {
		ptr := newTestCancelTokenHandle()
		mu.Lock()
		tokens[ptr] = &fakeToken{}
		mu.Unlock()
		return ptr
	})
	ffiCtx = context.WithValue(ffiCtx, ffiCancelTokenCancel.sym, func(ptr *opendalCancelToken) {
		mu.Lock()
		ft := tokens[ptr]
		mu.Unlock()
		ft.mu.Lock()
		if ft.freed {
			t.Errorf("use-after-free: cancel called on freed token %p", ptr)
		}
		ft.cancel = true
		ft.mu.Unlock()
	})
	ffiCtx = context.WithValue(ffiCtx, ffiCancelTokenFree.sym, func(ptr *opendalCancelToken) {
		mu.Lock()
		ft := tokens[ptr]
		mu.Unlock()
		ft.mu.Lock()
		if ft.freed {
			t.Errorf("double-free on token %p", ptr)
		}
		ft.freed = true
		ft.mu.Unlock()
	})

	const workers = 64
	const iters = 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				go cancel()
				_ = withCancelToken(ffiCtx, ctx, func(tok *opendalCancelToken) error {
					mu.Lock()
					ft := tokens[tok]
					mu.Unlock()
					ft.mu.Lock()
					cancelled := ft.cancel
					ft.mu.Unlock()
					if cancelled {
						return &Error{code: CodeCancelled, message: "operation cancelled"}
					}
					return nil
				})
				cancel()
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for ptr, ft := range tokens {
		if !ft.freed {
			t.Fatalf("leaked cancel token %p", ptr)
		}
	}
}

func TestLiveCCancelTokenLifecycleAndContextOperations(t *testing.T) {
	libPath := findLocalCLibrary(t)

	op, err := NewOperator(localTestScheme{name: "memory", path: libPath}, OperatorOptions{})
	if err != nil {
		t.Fatalf("NewOperator(memory): %v", err)
	}
	defer op.Close()

	// 1. Verify raw C cancel token lifecycle symbols directly.
	tok := ffiCancelTokenNew.symbol(op.ctx)()
	if tok == nil {
		t.Fatal("opendal_cancel_token_new returned nil")
	}
	if ffiCancelTokenIsCancelled.symbol(op.ctx)(tok) {
		t.Fatal("new cancel token reported cancelled")
	}
	ffiCancelTokenCancel.symbol(op.ctx)(tok)
	if !ffiCancelTokenIsCancelled.symbol(op.ctx)(tok) {
		t.Fatal("cancelled token did not report cancelled")
	}
	// Passing a cancelled native token directly into *_with_cancel must return OPENDAL_CANCELLED.
	if err := ffiOperatorCheck.symbol(op.ctx)(op.inner, tok); err == nil {
		t.Fatal("ffiOperatorCheck with cancelled token succeeded, want CodeCancelled")
	} else {
		var opErr *Error
		if !errors.As(err, &opErr) || opErr.Code() != CodeCancelled {
			t.Fatalf("ffiOperatorCheck with cancelled token = %v, want CodeCancelled", err)
		}
	}
	ffiCancelTokenFree.symbol(op.ctx)(tok)

	// 2. Active cancellable context succeeds end-to-end across memory operations.
	activeCtx, activeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer activeCancel()

	if _, err := op.Write(activeCtx, "hello.txt", []byte("hello")); err != nil {
		t.Fatalf("Write with timeout context: %v", err)
	}
	data, err := op.Read(activeCtx, "hello.txt")
	if err != nil || string(data) != "hello" {
		t.Fatalf("Read with timeout context = %q, %v", data, err)
	}

	// 3. Pre-cancelled context returns wrapped context.Canceled / context.DeadlineExceeded.
	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	_, err = op.Read(canceledCtx, "hello.txt")
	assertCanceledError(t, "live Read(canceled)", err, context.Canceled)

	expiredCtx, expireCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expireCancel()
	_, err = op.Stat(expiredCtx, "hello.txt")
	assertCanceledError(t, "live Stat(expired)", err, context.DeadlineExceeded)

	// 4. Concurrent cancellation stress against live libopendal_c.so (fs + memory).
	fsOp, err := NewOperator(localTestScheme{name: "fs", path: libPath}, OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatalf("NewOperator(fs): %v", err)
	}
	defer fsOp.Close()
	if _, err := fsOp.Write(activeCtx, "stress.txt", []byte("payload")); err != nil {
		t.Fatalf("fsOp.Write: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithCancel(context.Background())
				go cancel()
				_, err := fsOp.Read(ctx, "stress.txt")
				if err != nil {
					assertCanceledError(t, "concurrent live Read", err, context.Canceled)
				}
				cancel()
			}
		}()
	}
	wg.Wait()
}
