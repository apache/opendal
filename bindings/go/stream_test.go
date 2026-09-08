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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
)

type streamReaderFunc func([]byte) (int, error)

func (f streamReaderFunc) Read(p []byte) (int, error) { return f(p) }

type streamWriterFunc func([]byte) (int, error)

func (f streamWriterFunc) Write(p []byte) (int, error) { return f(p) }

func streamTestWriter(dst io.Writer) *Writer {
	write := func(_ *opendalWriter, p []byte) (int, error) { return dst.Write(p) }
	return &Writer{ctx: context.WithValue(context.Background(), ffiWriterWrite.opts.sym, write)}
}

func streamTestReader(src io.Reader) *Reader {
	read := func(_ *opendalReader, p []byte) (uint, error) {
		n, err := src.Read(p)
		if err == io.EOF && n == 0 {
			err = nil
		}
		return uint(n), err
	}
	return &Reader{ctx: context.WithValue(context.Background(), ffiReaderRead.opts.sym, read)}
}

func TestWriterReadFromFileAndReader(t *testing.T) {
	data := bytes.Repeat([]byte("transfer"), (1<<20)/8+1)
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"file", "plain_reader"} {
		for _, method := range []string{"Copy", "CopyBuffer", "ReadFrom"} {
			t.Run(source+"/"+method, func(t *testing.T) {
				var src io.Reader = struct{ io.Reader }{bytes.NewReader(data)}
				if source == "file" {
					f, err := os.Open(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := f.Close(); err != nil {
							t.Error(err)
						}
					})
					src = f
				}
				var got bytes.Buffer
				calls := 0
				w := streamTestWriter(streamWriterFunc(func(p []byte) (int, error) {
					calls++
					if len(p) > streamCopyBufferSize {
						t.Fatalf("write exceeds copy buffer bound: %d", len(p))
					}
					return got.Write(p)
				}))
				var n int64
				var err error
				switch method {
				case "Copy":
					n, err = io.Copy(w, src)
				case "CopyBuffer":
					n, err = io.CopyBuffer(w, src, make([]byte, 7))
				case "ReadFrom":
					n, err = w.ReadFrom(src)
				}
				if err != nil || n != int64(len(data)) || !bytes.Equal(got.Bytes(), data) {
					t.Fatalf("copy = (%d, %v), equal = %v", n, err, bytes.Equal(got.Bytes(), data))
				}
				if calls >= (len(data)+32767)/32768 {
					t.Fatalf("copy still uses at least as many writes as the 32 KiB fallback: %d", calls)
				}
			})
		}
	}
}

func TestReaderWriteToUsesCopyBuffer(t *testing.T) {
	data := bytes.Repeat([]byte("data"), (1<<20)/4+1)
	for _, method := range []string{"Copy", "CopyBuffer", "WriteTo"} {
		t.Run(method, func(t *testing.T) {
			src := bytes.NewReader(data)
			writes := 0
			r := streamTestReader(src)
			var got bytes.Buffer
			dst := streamWriterFunc(func(p []byte) (int, error) {
				writes++
				if len(p) > streamCopyBufferSize {
					t.Fatalf("write exceeds copy buffer bound: %d", len(p))
				}
				return got.Write(p)
			})
			var n int64
			var err error
			switch method {
			case "Copy":
				n, err = io.Copy(dst, r)
			case "CopyBuffer":
				n, err = io.CopyBuffer(dst, r, make([]byte, 7))
			case "WriteTo":
				n, err = r.WriteTo(dst)
			}
			if err != nil || n != int64(len(data)) || !bytes.Equal(got.Bytes(), data) {
				t.Fatalf("copy = (%d, %v), equal = %v", n, err, bytes.Equal(got.Bytes(), data))
			}
			if writes >= (len(data)+32767)/32768 {
				t.Fatalf("copy still uses at least as many writes as the 32 KiB fallback: %d", writes)
			}
		})
	}
}

func TestStreamCopyResults(t *testing.T) {
	readErr, writeErr := errors.New("read failed"), errors.New("write failed")
	for _, tc := range []struct {
		name     string
		data     string
		readErr  error
		short    bool
		writeErr error
		wantN    int64
		wantErr  error
	}{
		{"empty", "", io.EOF, false, nil, 0, nil},
		{"data_and_EOF", "abc", io.EOF, false, nil, 3, nil},
		{"read_error", "", readErr, false, nil, 0, readErr},
		{"data_and_read_error", "abc", readErr, false, nil, 3, readErr},
		{"full_buffer_and_error", string(make([]byte, streamCopyBufferSize)), readErr, false, nil, streamCopyBufferSize, readErr},
		{"short_write", "abc", io.EOF, true, nil, 2, io.ErrShortWrite},
		{"partial_write_error", "abc", io.EOF, true, writeErr, 2, writeErr},
		{"write_error_precedes_read_error", "abc", readErr, true, writeErr, 2, writeErr},
	} {
		for _, method := range []string{"ReadFrom", "WriteTo"} {
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				remaining := tc.data
				src := streamReaderFunc(func(p []byte) (int, error) {
					n := copy(p, remaining)
					remaining = remaining[n:]
					if remaining == "" {
						return n, tc.readErr
					}
					return n, nil
				})
				var got bytes.Buffer
				dst := streamWriterFunc(func(p []byte) (int, error) {
					if tc.short {
						p = p[:len(p)-1]
					}
					n, _ := got.Write(p)
					return n, tc.writeErr
				})
				var n int64
				var err error
				if method == "ReadFrom" {
					n, err = streamTestWriter(dst).ReadFrom(src)
				} else {
					n, err = streamTestReader(src).WriteTo(dst)
				}
				if n != tc.wantN || err != tc.wantErr || got.String() != tc.data[:tc.wantN] {
					t.Fatalf("copy = (%d, %v), want (%d, %v); written %d bytes", n, err, tc.wantN, tc.wantErr, got.Len())
				}
			})
		}
	}
}

func TestReaderWriteToRespectsBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := bytes.NewReader(make([]byte, streamCopyBufferSize*2))
		reads := 0
		r := streamTestReader(streamReaderFunc(func(p []byte) (int, error) {
			reads++
			return src.Read(p)
		}))
		blocked := make(chan struct{})
		defer close(blocked)
		dst := streamWriterFunc(func(p []byte) (int, error) {
			<-blocked
			return len(p), nil
		})
		go func() {
			if n, err := r.WriteTo(dst); n != 2*streamCopyBufferSize || err != nil {
				t.Errorf("WriteTo = (%d, %v)", n, err)
			}
		}()
		synctest.Wait()
		if reads != 1 {
			t.Fatalf("native reads while destination is blocked = %d, want 1", reads)
		}
	})
}

func TestStreamCopyLeavesCloseToCaller(t *testing.T) {
	var got bytes.Buffer
	w := streamTestWriter(&got)
	writerClosed, writerFreed, readerFreed := 0, 0, 0
	closeErr := errors.New("close failed")
	w.ctx = context.WithValue(w.ctx, ffiWriterCloseWithMetadata.opts.sym, func(_ *opendalWriter) (*opendalMetadata, error) {
		writerClosed++
		return nil, closeErr
	})
	w.ctx = context.WithValue(w.ctx, ffiWriterFree.opts.sym, func(_ *opendalWriter) { writerFreed++ })
	r := streamTestReader(bytes.NewReader([]byte("abc")))
	r.ctx = context.WithValue(r.ctx, ffiReaderFree.opts.sym, func(_ *opendalReader) { readerFreed++ })
	if n, err := io.Copy(w, r); n != 3 || err != nil || got.String() != "abc" {
		t.Fatalf("Copy = (%d, %v), data %q", n, err, got.String())
	}
	if _, err := w.ReadFrom(bytes.NewReader([]byte("def"))); err != nil {
		t.Fatal(err)
	}
	if writerClosed != 0 || writerFreed != 0 || readerFreed != 0 {
		t.Fatal("copy closed a stream")
	}
	if _, err := w.Close(); err != closeErr {
		t.Fatalf("Close = %v, want %v", err, closeErr)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if writerClosed != 1 || writerFreed != 1 || readerFreed != 1 {
		t.Fatalf("close calls: writer=%d/%d reader=%d", writerClosed, writerFreed, readerFreed)
	}
}

func TestReaderWriteToAfterSeek(t *testing.T) {
	src := bytes.NewReader([]byte("abcdefgh"))
	r := streamTestReader(src)
	r.ctx = context.WithValue(r.ctx, ffiReaderSeek.opts.sym, func(_ *opendalReader, offset int64, whence int) (int64, error) {
		return src.Seek(offset, whence)
	})
	if _, err := r.Seek(3, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	var dst bytes.Buffer
	if n, err := r.WriteTo(&dst); n != 5 || err != nil || dst.String() != "defgh" {
		t.Fatalf("WriteTo = (%d, %v), data %q", n, err, dst.String())
	}
	if pos, err := r.Seek(0, io.SeekCurrent); pos != 8 || err != nil {
		t.Fatalf("position after WriteTo = (%d, %v)", pos, err)
	}
}

func BenchmarkStreamCopyBuffer(b *testing.B) {
	data := make([]byte, 8<<20)
	for _, size := range []int{32 << 10, 64 << 10, 256 << 10, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			writes := 0
			w := streamTestWriter(streamWriterFunc(func(p []byte) (int, error) {
				writes++
				return len(p), nil
			}))
			src := bytes.NewReader(data)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				src.Reset(data)
				if _, err := io.CopyBuffer(struct{ io.Writer }{w}, struct{ io.Reader }{src}, make([]byte, size)); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(writes)/float64(b.N), "writes/op")
		})
	}
}
