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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type streamNativeScheme struct{ name, path string }

func (s streamNativeScheme) Name() string    { return s.name }
func (s streamNativeScheme) Path() string    { return s.path }
func (s streamNativeScheme) LoadOnce() error { return nil }

func streamNativeOperator(t testing.TB, service string) *Operator {
	t.Helper()
	path := os.Getenv("OPENDAL_GO_TEST_LIBRARY")
	if path == "" {
		t.Skip("set OPENDAL_GO_TEST_LIBRARY to a locally built C library with the fs service")
	}
	op, err := NewOperator(streamNativeScheme{service, path}, OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(op.Close)
	return op
}

func TestStreamNativeFileCopy(t *testing.T) {
	op := streamNativeOperator(t, "fs")
	data := bytes.Repeat([]byte("file transfer\n"), 100000)
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	src, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := src.Close(); err != nil {
			t.Error(err)
		}
	})
	w, err := op.Writer("copied")
	if err != nil {
		t.Fatal(err)
	}
	n, copyErr := io.Copy(w, src)
	meta, closeErr := w.Close()
	if copyErr != nil || closeErr != nil || n != int64(len(data)) {
		t.Fatalf("upload = (%d, %v), Close = %v", n, copyErr, closeErr)
	}
	if meta == nil {
		t.Fatal("Close did not return metadata")
	}
	r, err := op.Reader("copied")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := r.Seek(17, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	dst, err := os.Create(filepath.Join(t.TempDir(), "download"))
	if err != nil {
		t.Fatal(err)
	}
	n, copyErr = io.CopyBuffer(dst, r, make([]byte, 7))
	closeErr = dst.Close()
	if copyErr != nil || closeErr != nil || n != int64(len(data)-17) {
		t.Fatalf("download = (%d, %v), Close = %v", n, copyErr, closeErr)
	}
	got, err := os.ReadFile(dst.Name())
	if err != nil || !bytes.Equal(got, data[17:]) {
		t.Fatalf("download verification: error = %v, equal = %v", err, bytes.Equal(got, data[17:]))
	}
	if pos, err := r.Seek(0, io.SeekCurrent); err != nil || pos != int64(len(data)) {
		t.Fatalf("position after copy = (%d, %v)", pos, err)
	}
}

func TestStreamNativeBufferOwnership(t *testing.T) {
	op := streamNativeOperator(t, "memory")
	w, err := op.Writer("owned")
	if err != nil {
		t.Fatal(err)
	}
	// Each block has a different value.
	// This detects a write that keeps the buffer after the next read changes it.
	data := make([]byte, streamCopyBufferSize*3+19)
	for i := range data {
		data[i] = byte(i/streamCopyBufferSize + 1)
	}
	_, copyErr := w.ReadFrom(bytes.NewReader(data))
	tail := []byte("caller owned tail")
	_, writeErr := w.Write(tail)
	want := append(append([]byte(nil), data...), tail...)
	clear(tail)
	clear(data)
	_, closeErr := w.Close()
	if copyErr != nil || writeErr != nil || closeErr != nil {
		t.Fatalf("copy = %v, write = %v, close = %v", copyErr, writeErr, closeErr)
	}
	got, err := op.Read("owned")
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("ownership verification: error = %v, equal = %v", err, bytes.Equal(got, want))
	}
}

func BenchmarkNativeStreamUpload(b *testing.B) {
	op := streamNativeOperator(b, "fs")
	data := make([]byte, 16<<20)
	path := filepath.Join(b.TempDir(), "source")
	if err := os.WriteFile(path, data, 0600); err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{0, 32 << 10, 64 << 10, 256 << 10, 1 << 20} {
		name := "io.Copy"
		if size != 0 {
			name = fmt.Sprint(size)
		}
		b.Run(name, func(b *testing.B) {
			file, err := os.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := file.Close(); err != nil {
					b.Error(err)
				}
			})
			writes := 0
			var firstWrite time.Duration
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := file.Seek(0, io.SeekStart); err != nil {
					b.Fatal(err)
				}
				w, err := op.Writer("benchmark")
				if err != nil {
					b.Fatal(err)
				}
				write := ffiWriterWrite.symbol(w.ctx)
				first := true
				start := time.Now()
				w.ctx = context.WithValue(w.ctx, ffiWriterWrite.opts.sym, func(inner *opendalWriter, p []byte) (int, error) {
					if first {
						firstWrite += time.Since(start)
						first = false
					}
					writes++
					return write(inner, p)
				})
				var copyErr error
				if size == 0 {
					_, copyErr = io.Copy(w, file)
				} else {
					_, copyErr = io.CopyBuffer(struct{ io.Writer }{w}, struct{ io.Reader }{file}, make([]byte, size))
				}
				_, closeErr := w.Close()
				if copyErr != nil || closeErr != nil {
					b.Fatalf("copy = %v, close = %v", copyErr, closeErr)
				}
			}
			b.ReportMetric(float64(writes)/float64(b.N), "writes/op")
			b.ReportMetric(float64(firstWrite.Nanoseconds())/float64(b.N), "first-write-ns/op")
		})
	}
}

func BenchmarkNativeStreamDownload(b *testing.B) {
	op := streamNativeOperator(b, "fs")
	data := make([]byte, 16<<20)
	if _, err := op.Write("source", data); err != nil {
		b.Fatal(err)
	}
	file, err := os.Create(filepath.Join(b.TempDir(), "download"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := file.Close(); err != nil {
			b.Error(err)
		}
	})
	reads, writes := 0, 0
	var firstWrite time.Duration
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		r, err := op.Reader("source")
		if err != nil {
			b.Fatal(err)
		}
		read := ffiReaderRead.symbol(r.ctx)
		r.ctx = context.WithValue(r.ctx, ffiReaderRead.opts.sym, func(inner *opendalReader, p []byte) (uint, error) {
			reads++
			return read(inner, p)
		})
		first := true
		start := time.Now()
		dst := streamNativeWriterFunc(func(p []byte) (int, error) {
			if first {
				firstWrite += time.Since(start)
				first = false
			}
			writes++
			return file.Write(p)
		})
		_, copyErr := io.Copy(dst, r)
		closeErr := r.Close()
		if copyErr != nil || closeErr != nil {
			b.Fatalf("copy = %v, close = %v", copyErr, closeErr)
		}
	}
	b.ReportMetric(float64(reads)/float64(b.N), "native-reads/op")
	b.ReportMetric(float64(writes)/float64(b.N), "writes/op")
	b.ReportMetric(float64(firstWrite.Nanoseconds())/float64(b.N), "first-write-ns/op")
}

type streamNativeWriterFunc func([]byte) (int, error)

func (f streamNativeWriterFunc) Write(p []byte) (int, error) { return f(p) }
