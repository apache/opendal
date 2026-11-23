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
	"slices"
	"testing"
	"unsafe"
)

func TestDeleterCreationReportsAndFreesNativeError(t *testing.T) {
	nativeErr := &opendalError{code: int32(CodeUnsupported)}
	freed := 0
	ctx := context.WithValue(context.Background(), ffiErrorFree.sym, func(err *opendalError) {
		if err != nativeErr {
			t.Fatalf("freed error = %p, want %p", err, nativeErr)
		}
		freed++
	})
	ctx = context.WithValue(ctx, ffiOperatorDeleter.sym, ffiOperatorDeleter.withFunc(ctx, func(_ *opendalOperator, _ *opendalCancelToken, out **opendalDeleter) *opendalError {
		*out = nil
		return nativeErr
	}))

	op := &Operator{ctx: ctx, inner: &opendalOperator{}}
	deleter, err := op.Deleter(context.Background())
	var opErr *Error
	if deleter != nil || !errors.As(err, &opErr) || opErr.Code() != CodeUnsupported {
		t.Fatalf("Deleter() = %v, %v, want nil and CodeUnsupported", deleter, err)
	}
	if freed != 1 {
		t.Fatalf("Deleter freed the native error %d times, want 1", freed)
	}
}

func TestFfiDeleterDeleteManyMarshalsPaths(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	want := []string{"a.txt", "b.txt", "nested/c.txt"}
	calls := 0

	deleteMany := ffiDeleterDeleteMany.withFunc(context.Background(), func(d *opendalDeleter, paths **byte, pathsLen uint, _ *opendalCancelToken) *opendalError {
		calls++
		if d != deleterInner {
			t.Fatalf("deleter = %p, want %p", d, deleterInner)
		}
		if pathsLen != uint(len(want)) {
			t.Fatalf("paths len = %d, want %d", pathsLen, len(want))
		}
		for i, path := range unsafe.Slice(paths, pathsLen) {
			if got := BytePtrToString(path); got != want[i] {
				t.Fatalf("path[%d] = %q, want %q", i, got, want[i])
			}
		}
		return nil
	})

	if err := deleteMany(deleterInner, want, nil); err != nil {
		t.Fatalf("delete many failed: %v", err)
	}
	// One crossing for the whole slice is the point of this entry point.
	if calls != 1 {
		t.Fatalf("delete many called FFI %d times, want 1", calls)
	}
}

func TestFfiDeleterDeleteManyRejectsNulBeforeCall(t *testing.T) {
	called := false
	deleteMany := ffiDeleterDeleteMany.withFunc(context.Background(), func(*opendalDeleter, **byte, uint, *opendalCancelToken) *opendalError {
		called = true
		return nil
	})

	err := deleteMany(newTestDeleterHandle(), []string{"a.txt", "bad\x00path"}, nil)
	if err == nil {
		t.Fatal("delete many with a nul path succeeded")
	}
	if called {
		t.Fatal("delete many called FFI after path validation failed")
	}
}

func TestDeleterDeleteWithOptionsKeepsStringsUntilCall(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	cOpts := &opendalDeleteOptions{}
	var versionPtr *byte
	var recursive bool
	optionsFreed := 0

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiDeleteOptionsNew.sym, func() *opendalDeleteOptions { return cOpts })
	ctx = context.WithValue(ctx, ffiDeleteOptionsFree.sym, func(opts *opendalDeleteOptions) {
		assertDeleteOptionsPointer(t, cOpts, opts)
		optionsFreed++
	})
	ctx = context.WithValue(ctx, ffiDeleteOptionsSetRecursive.sym, func(opts *opendalDeleteOptions, value bool) {
		assertDeleteOptionsPointer(t, cOpts, opts)
		recursive = value
	})
	ctx = context.WithValue(ctx, ffiDeleteOptionsSetVersion.sym, ffiDeleteOptionsSetVersion.withFunc(ctx, func(opts *opendalDeleteOptions, value *byte) {
		assertDeleteOptionsPointer(t, cOpts, opts)
		versionPtr = value
	}))
	ctx = context.WithValue(ctx, ffiDeleterDelete.sym, ffiDeleterDelete.withFunc(ctx, func(d *opendalDeleter, path *byte, opts *opendalDeleteOptions, _ *opendalCancelToken) *opendalError {
		if d != deleterInner {
			t.Fatalf("deleter = %p, want %p", d, deleterInner)
		}
		if got := BytePtrToString(path); got != "file.txt" {
			t.Fatalf("path = %q, want file.txt", got)
		}
		assertDeleteOptionsPointer(t, cOpts, opts)
		if !recursive {
			t.Fatal("delete recursive = false, want true")
		}
		if got := BytePtrToString(versionPtr); got != "v1" {
			t.Fatalf("delete version pointer = %q, want v1", got)
		}
		return nil
	}))

	deleter := &Deleter{inner: deleterInner, ctx: ctx}
	if err := deleter.Delete(context.Background(), "file.txt", DeleteWithVersion("v1"), DeleteWithRecursive(true)); err != nil {
		t.Fatalf("Delete with options failed: %v", err)
	}
	if optionsFreed != 1 {
		t.Fatalf("delete options freed %d times, want 1", optionsFreed)
	}
}

func TestDeleterCloseFlushesFreesAndBlocksReuse(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	var calls deleterCalls

	ctx := newDeleterTestContext(t, deleterInner, &calls, nil, nil)

	deleter := &Deleter{inner: deleterInner, ctx: ctx}
	if err := deleter.Close(context.Background()); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := deleter.Close(context.Background()); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if calls.flushes != 1 {
		t.Fatalf("deleter flushed %d times, want 1", calls.flushes)
	}
	if calls.freed != 1 {
		t.Fatalf("deleter freed %d times, want 1", calls.freed)
	}
	if err := deleter.Delete(context.Background(), "file.txt"); !errors.Is(err, ErrDeleterClosed) {
		t.Fatalf("Delete after Close = %v, want ErrDeleterClosed", err)
	}
}

func TestDeleterCloseReleasesAndReportsFailedFlush(t *testing.T) {
	// Close releases unconditionally, like every other Close in this package, so
	// a deferred Close cannot leak the handle. Retrying is Flush's job.
	deleterInner := newTestDeleterHandle()
	flushErr := errors.New("flush failed")
	var calls deleterCalls

	ctx := newDeleterTestContext(t, deleterInner, &calls, nil, func() error { return flushErr })

	deleter := &Deleter{inner: deleterInner, ctx: ctx}
	if err := deleter.Close(context.Background()); !errors.Is(err, flushErr) {
		t.Fatalf("Close = %v, want flush error", err)
	}
	if calls.freed != 1 {
		t.Fatalf("deleter freed %d times after failed flush, want 1", calls.freed)
	}
	if err := deleter.Close(context.Background()); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if calls.freed != 1 {
		t.Fatalf("deleter freed %d times after the second Close, want 1", calls.freed)
	}
}

func TestDeleterFlushRetriesThenCloseReleases(t *testing.T) {
	// Flush keeps the deleter alive so a failed flush can be retried; only the
	// Close that follows releases it.
	deleterInner := newTestDeleterHandle()
	flushErr := errors.New("flush failed")
	var calls deleterCalls

	ctx := newDeleterTestContext(t, deleterInner, &calls, nil, func() error {
		if calls.flushes == 1 {
			return flushErr
		}
		return nil
	})

	deleter := &Deleter{inner: deleterInner, ctx: ctx}
	if err := deleter.Flush(context.Background()); !errors.Is(err, flushErr) {
		t.Fatalf("Flush = %v, want flush error", err)
	}
	if calls.freed != 0 {
		t.Fatalf("Flush freed the deleter %d times, want 0", calls.freed)
	}
	if err := deleter.Flush(context.Background()); err != nil {
		t.Fatalf("Flush retry failed: %v", err)
	}
	if err := deleter.Close(context.Background()); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if calls.flushes != 3 {
		t.Fatalf("flushed %d times, want 3", calls.flushes)
	}
	if calls.freed != 1 {
		t.Fatalf("deleter freed %d times, want 1", calls.freed)
	}
}

func TestDeleterDiscardReleasesWithoutFlushing(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	var calls deleterCalls

	ctx := newDeleterTestContext(t, deleterInner, &calls, nil, nil)

	deleter := &Deleter{inner: deleterInner, ctx: ctx}
	deleter.Discard()
	deleter.Discard()
	if calls.flushes != 0 {
		t.Fatalf("Discard flushed %d times, want 0", calls.flushes)
	}
	if calls.freed != 1 {
		t.Fatalf("deleter freed %d times, want 1", calls.freed)
	}
	if err := deleter.Delete(context.Background(), "file.txt"); !errors.Is(err, ErrDeleterClosed) {
		t.Fatalf("Delete after Discard = %v, want ErrDeleterClosed", err)
	}
}

func TestDeleterFlushKeepsDeleterUsable(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	var calls deleterCalls

	ctx := newDeleterTestContext(t, deleterInner, &calls, nil, nil)

	deleter := &Deleter{inner: deleterInner, ctx: ctx}
	if err := deleter.Flush(context.Background()); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if calls.freed != 0 {
		t.Fatalf("Flush freed the deleter %d times, want 0", calls.freed)
	}
	if err := deleter.Delete(context.Background(), "file.txt"); err != nil {
		t.Fatalf("Delete after Flush failed: %v", err)
	}
	if err := deleter.Close(context.Background()); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if calls.flushes != 2 {
		t.Fatalf("flushed %d times, want 2", calls.flushes)
	}
	if err := deleter.Flush(context.Background()); !errors.Is(err, ErrDeleterClosed) {
		t.Fatalf("Flush after Close = %v, want ErrDeleterClosed", err)
	}
}

func TestRemoveQueuesEveryPathInOneCrossingThenCloses(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	var calls deleterCalls

	ctx := newDeleterTestContext(t, deleterInner, &calls, nil, nil)

	op := &Operator{ctx: ctx, inner: &opendalOperator{}}
	if err := op.Remove(context.Background(), []string{"a.txt", "b.txt", "c.txt"}); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	assertPaths(t, calls.deleted, []string{"a.txt", "b.txt", "c.txt"})
	// Remove exists to hand the whole slice over at once. Queueing per path would
	// still pass the assertion above, so pin the crossing count down too.
	if calls.bulk != 1 {
		t.Fatalf("Remove made %d bulk calls, want 1", calls.bulk)
	}
	if calls.perPath != 0 {
		t.Fatalf("Remove queued %d paths one at a time, want 0", calls.perPath)
	}
	if calls.flushes != 1 || calls.freed != 1 {
		t.Fatalf("deleter flushed %d times and freed %d times, want 1 and 1", calls.flushes, calls.freed)
	}
}

func TestRemoveDiscardsQueueWhenQueueingFails(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	var calls deleterCalls
	deleteErr := errors.New("delete failed")

	ctx := newDeleterTestContext(t, deleterInner, &calls, func([]string) error { return deleteErr }, nil)

	op := &Operator{ctx: ctx, inner: &opendalOperator{}}
	if err := op.Remove(context.Background(), []string{"a.txt", "b.txt", "c.txt"}); !errors.Is(err, deleteErr) {
		t.Fatalf("Remove = %v, want delete error", err)
	}
	// Flushing here would delete whatever was queued before the failure while
	// reporting that Remove failed.
	if calls.flushes != 0 {
		t.Fatalf("Remove flushed %d times on its error path, want 0", calls.flushes)
	}
	if calls.freed != 1 {
		t.Fatalf("deleter freed %d times, want 1", calls.freed)
	}
}

func assertPaths(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("queued %v, want %v", got, want)
	}
}

func TestWithDeleterReportsAndReleasesOnFailedFlush(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	flushErr := errors.New("flush failed")
	var calls deleterCalls

	ctx := newDeleterTestContext(t, deleterInner, &calls, nil, func() error { return flushErr })

	op := &Operator{ctx: ctx, inner: &opendalOperator{}}
	err := op.WithDeleter(context.Background(), func(d *Deleter) error { return d.Delete(context.Background(), "a.txt") })
	if !errors.Is(err, flushErr) {
		t.Fatalf("WithDeleter = %v, want flush error", err)
	}
	if calls.freed != 1 {
		t.Fatalf("deleter freed %d times, want 1", calls.freed)
	}
}

func TestWithDeleterReleasesWhenFnPanics(t *testing.T) {
	deleterInner := newTestDeleterHandle()
	var calls deleterCalls

	ctx := newDeleterTestContext(t, deleterInner, &calls, nil, nil)

	op := &Operator{ctx: ctx, inner: &opendalOperator{}}
	panicValue := errors.New("callback panicked")
	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Fatalf("recovered %v, want %v", got, panicValue)
			}
		}()
		_ = op.WithDeleter(context.Background(), func(*Deleter) error { panic(panicValue) })
	}()

	if calls.freed != 1 {
		t.Fatalf("deleter freed %d times after a panic, want 1", calls.freed)
	}
	if calls.flushes != 0 {
		t.Fatalf("flushed %d times after a panic, want 0", calls.flushes)
	}
}

func TestRemoveWithoutPathsSkipsDeleter(t *testing.T) {
	ctx := context.WithValue(context.Background(), ffiOperatorDeleter.sym, func(*opendalOperator, *opendalCancelToken) (*opendalDeleter, error) {
		t.Fatal("Remove(nil) created a deleter")
		return nil, nil
	})

	op := &Operator{ctx: ctx, inner: &opendalOperator{}}
	if err := op.Remove(context.Background(), nil); err != nil {
		t.Fatalf("Remove(nil) = %v, want nil", err)
	}
}

// newTestDeleterHandle returns a distinct stand-in for a C deleter handle.
//
// opendalDeleter is zero-sized, so every &opendalDeleter{} shares runtime.zerobase
// and the "is this the handle I was given?" assertions below would hold no matter
// which handle the code passed. Backing each handle with a byte gives it its own
// address and makes those assertions real.
func newTestDeleterHandle() *opendalDeleter {
	return (*opendalDeleter)(unsafe.Pointer(new(byte)))
}

// deleterCalls records what the stubbed deleter FFI saw. Separating bulk from
// perPath is what lets a test tell "queued the slice at once" apart from "queued
// the same paths one crossing at a time".
type deleterCalls struct {
	deleted []string
	bulk    int
	perPath int
	flushes int
	freed   int
}

func newDeleterTestContext(t *testing.T, inner *opendalDeleter, calls *deleterCalls, deleteFn func([]string) error, flushFn func() error) context.Context {
	t.Helper()

	ctx := context.Background()
	ctx = context.WithValue(ctx, ffiOperatorDeleter.sym, ffiOperatorDeleter.withFunc(ctx, func(op *opendalOperator, _ *opendalCancelToken, out **opendalDeleter) *opendalError {
		if op == nil {
			t.Fatal("Deleter() passed nil operator")
		}
		*out = inner
		return nil
	}))
	ctx = context.WithValue(ctx, ffiDeleterDelete.sym, func(d *opendalDeleter, path string, _ *opendalDeleteOptions, _ *opendalCancelToken) error {
		if d != inner {
			t.Fatalf("deleter = %p, want %p", d, inner)
		}
		calls.perPath++
		calls.deleted = append(calls.deleted, path)
		if deleteFn != nil {
			return deleteFn([]string{path})
		}
		return nil
	})
	ctx = context.WithValue(ctx, ffiDeleterDeleteMany.sym, func(d *opendalDeleter, paths []string, _ *opendalCancelToken) error {
		if d != inner {
			t.Fatalf("deleter = %p, want %p", d, inner)
		}
		calls.bulk++
		calls.deleted = append(calls.deleted, paths...)
		if deleteFn != nil {
			return deleteFn(paths)
		}
		return nil
	})
	ctx = context.WithValue(ctx, ffiDeleterFlush.sym, func(d *opendalDeleter, _ *opendalCancelToken) error {
		if d != inner {
			t.Fatalf("flushed deleter = %p, want %p", d, inner)
		}
		// calls.flushes is incremented before flushFn runs, so a flushFn can key
		// its behavior off the attempt number.
		calls.flushes++
		if flushFn != nil {
			return flushFn()
		}
		return nil
	})
	ctx = context.WithValue(ctx, ffiDeleterFree.sym, func(d *opendalDeleter) {
		if d != inner {
			t.Fatalf("freed deleter = %p, want %p", d, inner)
		}
		calls.freed++
	})
	return ctx
}
