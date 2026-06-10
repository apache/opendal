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
	"errors"
	"testing"
	"time"
)

func newCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestOperatorContextFirstMethodsReturnCanceledContext(t *testing.T) {
	ctx := newCanceledContext()
	op := &Operator{}

	if _, err := op.Read(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read() error = %v, want context.Canceled", err)
	}
	if err := op.Write(ctx, "path", []byte("data")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write() error = %v, want context.Canceled", err)
	}
	if _, err := op.Stat(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat() error = %v, want context.Canceled", err)
	}
	if _, err := op.IsExist(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("IsExist() error = %v, want context.Canceled", err)
	}
	if err := op.Delete(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete() error = %v, want context.Canceled", err)
	}
	if _, err := op.List(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("List() error = %v, want context.Canceled", err)
	}
	if err := op.Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Check() error = %v, want context.Canceled", err)
	}
	if err := op.CreateDir(ctx, "path/"); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateDir() error = %v, want context.Canceled", err)
	}
	if _, err := op.Reader(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reader() error = %v, want context.Canceled", err)
	}
	if _, err := op.Writer(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Writer() error = %v, want context.Canceled", err)
	}
	if err := op.Copy(ctx, "src", "dest"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy() error = %v, want context.Canceled", err)
	}
	if err := op.Rename(ctx, "src", "dest"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Rename() error = %v, want context.Canceled", err)
	}
	if _, err := op.PresignRead(ctx, "path", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("PresignRead() error = %v, want context.Canceled", err)
	}
	if _, err := op.PresignWrite(ctx, "path", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("PresignWrite() error = %v, want context.Canceled", err)
	}
	if _, err := op.PresignDelete(ctx, "path", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("PresignDelete() error = %v, want context.Canceled", err)
	}
	if _, err := op.PresignStat(ctx, "path", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("PresignStat() error = %v, want context.Canceled", err)
	}
}

func TestWriterCloseWithContextPreCancelledPreservesHandle(t *testing.T) {
	inner := &opendalWriter{}
	w := &Writer{inner: inner, cancelCtx: newCanceledContext()}

	if err := w.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want context.Canceled", err)
	}
	if w.inner != inner {
		t.Fatal("pre-cancelled close should not release writer handle")
	}
}

func TestWriterCloseIsIdempotent(t *testing.T) {
	w := &Writer{}

	if err := w.Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
}

func TestWriterFreeIsIdempotent(t *testing.T) {
	w := &Writer{inner: &opendalWriter{}}

	w.mu.Lock()
	w.inner = nil
	w.mu.Unlock()

	w.free()
	w.free()
}

func TestWriterCloseShouldReleaseAfterClose(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "success", err: nil, want: true},
		{name: "native error", err: errors.New("close failed"), want: true},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldReleaseWriterAfterClose(tc.err); got != tc.want {
				t.Fatalf("shouldReleaseWriterAfterClose(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWriterDeferredCloseAfterPreCancelledClose(t *testing.T) {
	inner := &opendalWriter{}
	w := &Writer{inner: inner, cancelCtx: newCanceledContext()}

	if err := w.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want context.Canceled", err)
	}
	if w.inner != inner {
		t.Fatal("pre-cancelled close should preserve writer handle for deferred close")
	}
}

// TestInFlightCancellationBlocksUntilNativeReturns verifies the blocking
// cancellation contract: the binding must not return to the caller before the
// native FFI call finishes.
//
// This test only exercises the pre-cancel fast path (ctx already done before
// the FFI call starts) because a true in-flight test requires a live backend
// (e.g. OPENDAL_TEST=s3) to produce a long-running native call that can be
// interrupted mid-way. Running the behavior suite with a timeout context and
// asserting the goroutine count does not increase is a recommended integration
// check before release.
func TestPreCancelReturnsCanceled(t *testing.T) {
	ctx := newCanceledContext()

	op := &Operator{}
	if _, err := op.Read(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read with pre-cancelled ctx = %v, want context.Canceled", err)
	}
	if err := op.Write(ctx, "path", []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write with pre-cancelled ctx = %v, want context.Canceled", err)
	}
}

func TestIOHandleMethodsUseBoundCanceledContext(t *testing.T) {
	ctx := newCanceledContext()

	reader := &Reader{cancelCtx: ctx}
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reader.Read() error = %v, want context.Canceled", err)
	}
	if _, err := reader.Seek(0, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reader.Seek() error = %v, want context.Canceled", err)
	}

	writer := &Writer{cancelCtx: ctx}
	if _, err := writer.Write([]byte("data")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Writer.Write() error = %v, want context.Canceled", err)
	}

	lister := &Lister{cancelCtx: ctx}
	if lister.Next() {
		t.Fatal("Lister.Next() = true, want false")
	}
	if !errors.Is(lister.Error(), context.Canceled) {
		t.Fatalf("Lister.Error() = %v, want context.Canceled", lister.Error())
	}
}
