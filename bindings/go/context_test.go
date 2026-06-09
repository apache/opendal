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

func TestOperatorWithContextMethodsReturnCanceledContext(t *testing.T) {
	ctx := newCanceledContext()
	op := &Operator{}

	if _, err := op.ReadWithContext(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadWithContext() error = %v, want context.Canceled", err)
	}
	if err := op.WriteWithContext(ctx, "path", []byte("data")); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.StatWithContext(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("StatWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.IsExistWithContext(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("IsExistWithContext() error = %v, want context.Canceled", err)
	}
	if err := op.DeleteWithContext(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.ListWithContext(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListWithContext() error = %v, want context.Canceled", err)
	}
	if err := op.CheckWithContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CheckWithContext() error = %v, want context.Canceled", err)
	}
	if err := op.CreateDirWithContext(ctx, "path/"); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateDirWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.ReaderWithContext(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReaderWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.WriterWithContext(ctx, "path"); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriterWithContext() error = %v, want context.Canceled", err)
	}
	if err := op.CopyWithContext(ctx, "src", "dest"); !errors.Is(err, context.Canceled) {
		t.Fatalf("CopyWithContext() error = %v, want context.Canceled", err)
	}
	if err := op.RenameWithContext(ctx, "src", "dest"); !errors.Is(err, context.Canceled) {
		t.Fatalf("RenameWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.PresignReadWithContext(ctx, "path", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("PresignReadWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.PresignWriteWithContext(ctx, "path", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("PresignWriteWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.PresignDeleteWithContext(ctx, "path", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("PresignDeleteWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := op.PresignStatWithContext(ctx, "path", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("PresignStatWithContext() error = %v, want context.Canceled", err)
	}
}

func TestCancellableReadBufferUsesHeapForCancelableContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := make([]byte, 8)
	readBuf, apply := cancellableReadBuffer(ctx, buf)
	if len(readBuf) != len(buf) {
		t.Fatalf("readBuf len = %d, want %d", len(readBuf), len(buf))
	}
	if &readBuf[0] == &buf[0] {
		t.Fatal("expected heap buffer for cancelable context")
	}

	readBuf[0] = 'x'
	apply(1)
	if buf[0] != 'x' {
		t.Fatalf("buf[0] = %q, want %q", buf[0], 'x')
	}
}

func TestCancellableReadBufferReusesCallerBufferForBackgroundContext(t *testing.T) {
	buf := make([]byte, 8)
	readBuf, apply := cancellableReadBuffer(context.Background(), buf)
	if &readBuf[0] != &buf[0] {
		t.Fatal("expected caller buffer for non-cancelable context")
	}
	apply(0)
}

func TestCancellableWriteBufferCopiesForCancelableContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data := []byte("hello")
	copied := cancellableWriteBuffer(ctx, data)
	if &copied[0] == &data[0] {
		t.Fatal("expected heap copy for cancelable context")
	}
	copied[0] = 'H'
	if data[0] != 'h' {
		t.Fatalf("data[0] = %q, want %q", data[0], 'h')
	}
}

func TestWriterCloseWithContextPreCancelledPreservesHandle(t *testing.T) {
	inner := &opendalWriter{}
	w := &Writer{inner: inner}

	if err := w.CloseWithContext(newCanceledContext()); !errors.Is(err, context.Canceled) {
		t.Fatalf("CloseWithContext() error = %v, want context.Canceled", err)
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
	if err := w.CloseWithContext(context.Background()); err != nil {
		t.Fatalf("second CloseWithContext() = %v, want nil", err)
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

func TestWriterDeferredCloseAfterPreCancelledClose(t *testing.T) {
	inner := &opendalWriter{}
	w := &Writer{inner: inner}

	if err := w.CloseWithContext(newCanceledContext()); !errors.Is(err, context.Canceled) {
		t.Fatalf("CloseWithContext() error = %v, want context.Canceled", err)
	}
	if w.inner != inner {
		t.Fatal("pre-cancelled close should preserve writer handle for deferred close")
	}
}

func TestIOWithContextMethodsReturnCanceledContext(t *testing.T) {
	ctx := newCanceledContext()

	reader := &Reader{}
	if _, err := reader.ReadWithContext(ctx, make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reader.ReadWithContext() error = %v, want context.Canceled", err)
	}
	if _, err := reader.SeekWithContext(ctx, 0, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reader.SeekWithContext() error = %v, want context.Canceled", err)
	}

	writer := &Writer{}
	if _, err := writer.WriteWithContext(ctx, []byte("data")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Writer.WriteWithContext() error = %v, want context.Canceled", err)
	}

	lister := &Lister{}
	if lister.NextWithContext(ctx) {
		t.Fatal("Lister.NextWithContext() = true, want false")
	}
	if !errors.Is(lister.Error(), context.Canceled) {
		t.Fatalf("Lister.Error() = %v, want context.Canceled", lister.Error())
	}
}
