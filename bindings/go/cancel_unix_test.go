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

//go:build unix

package opendal

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestLiveCMidFlightCancellation(t *testing.T) {
	libPath := findLocalCLibrary(t)

	root := t.TempDir()
	const fifoName = "blocked.fifo"
	fifoPath := filepath.Join(root, fifoName)
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		t.Fatalf("Mkfifo(%q): %v", fifoPath, err)
	}

	op, err := NewOperator(localTestScheme{name: "fs", path: libPath}, OperatorOptions{"root": root})
	if err != nil {
		t.Fatalf("NewOperator(fs): %v", err)
	}
	t.Cleanup(func() {
		op.Close()
	})
	// Registered after t.TempDir() and op.Close() so LIFO cleanup unblocks any
	// Tokio spawn_blocking thread waiting in open(O_RDONLY) on the FIFO before
	// closing the operator and removing the directory.
	t.Cleanup(func() {
		if f, err := os.OpenFile(fifoPath, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			_ = f.Close()
		}
	})

	const timeout = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	// Opening a FIFO with O_RDONLY and no writer blocks inside fs's native
	// open() call until the context deadline triggers opendal_cancel_token_cancel.
	start := time.Now()
	_, err = op.Read(ctx, fifoName)
	elapsed := time.Since(start)

	assertCanceledError(t, "mid-flight live Read(fifo)", err, context.DeadlineExceeded)
	if elapsed < 100*time.Millisecond || elapsed >= 2*time.Second {
		t.Fatalf("mid-flight Read(fifo) returned after %v, want in [100ms, 2s)", elapsed)
	}

	// Verify the operator remains healthy for subsequent operations.
	verifyCtx, verifyCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer verifyCancel()
	if _, err := op.Write(verifyCtx, "ok.txt", []byte("still alive")); err != nil {
		t.Fatalf("Write after mid-flight cancellation: %v", err)
	}
	got, err := op.Read(verifyCtx, "ok.txt")
	if err != nil || string(got) != "still alive" {
		t.Fatalf("Read after mid-flight cancellation = %q, %v", got, err)
	}
}
