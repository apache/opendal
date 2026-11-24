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
	"math"
	"strconv"
	"testing"
	"testing/synctest"
)

func TestReaderReturnsAvailablePrefix(t *testing.T) {
	for _, size := range []int{8, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				blocked := make(chan struct{})
				defer close(blocked)
				calls := 0
				read := func(_ *opendalReader, p []byte, _ *opendalCancelToken) (uint, error) {
					calls++
					if calls == 1 {
						return uint(copy(p, "abc")), nil
					}
					<-blocked
					return 0, nil
				}
				r := &Reader{ctx: context.WithValue(context.Background(), ffiReaderRead.sym, read), inner: newTestReaderHandle()}
				done := make(chan struct{})
				buf := make([]byte, size)
				var n int
				var err error
				go func() {
					n, err = r.Read(buf)
					close(done)
				}()
				synctest.Wait()
				select {
				case <-done:
					if n != 3 || err != nil || string(buf[:n]) != "abc" {
						t.Fatalf("Read = (%d, %v), data %q", n, err, buf[:n])
					}
				default:
					t.Fatal("Read withheld the available prefix while waiting for more data")
				}
				// The consumer can stop after the first read.
				// The Reader must not read more data until the consumer calls Read again.
				synctest.Wait()
				if calls != 1 {
					t.Fatalf("native reads = %d, want 1", calls)
				}
			})
		})
	}
}

func TestReaderCopyWritesAvailablePrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blocked := make(chan struct{})
		defer close(blocked)
		calls := 0
		read := func(_ *opendalReader, p []byte, _ *opendalCancelToken) (uint, error) {
			calls++
			if calls == 1 {
				return uint(copy(p, "abc")), nil
			}
			<-blocked
			return 0, nil
		}
		r := &Reader{ctx: context.WithValue(context.Background(), ffiReaderRead.sym, read), inner: newTestReaderHandle()}
		var dst bytes.Buffer
		go func() {
			if n, err := io.Copy(struct{ io.Writer }{&dst}, r); n != 3 || err != nil {
				t.Errorf("Copy = (%d, %v)", n, err)
			}
		}()
		synctest.Wait()
		if dst.String() != "abc" {
			t.Fatal("Copy withheld the available bytes while waiting for more data")
		}
	})
}

func TestReaderReadResults(t *testing.T) {
	failure := errors.New("read failed")
	for _, tc := range []struct {
		name    string
		size    int
		data    string
		err     error
		wantN   int
		wantErr error
	}{
		{"empty_buffer", 0, "abc", nil, 0, nil},
		{"one_byte", 1, "abc", nil, 1, nil},
		{"short_read", 8, "abc", nil, 3, nil},
		{"full_buffer", 3, "abc", nil, 3, nil},
		{"EOF", 8, "", nil, 0, io.EOF},
		{"error", 8, "", failure, 0, failure},
		{"data_and_error", 8, "abc", failure, 3, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			read := func(_ *opendalReader, p []byte, _ *opendalCancelToken) (uint, error) {
				calls++
				return uint(copy(p, tc.data)), tc.err
			}
			r := &Reader{ctx: context.WithValue(context.Background(), ffiReaderRead.sym, read), inner: newTestReaderHandle()}
			buf := make([]byte, tc.size)
			n, err := r.Read(buf)
			if n != tc.wantN || err != tc.wantErr || string(buf[:n]) != tc.data[:n] {
				t.Fatalf("Read = (%d, %v), data %q", n, err, buf[:n])
			}
			wantCalls := 1
			if tc.size == 0 {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("native reads = %d, want %d", calls, wantCalls)
			}
		})
	}
}

func TestReaderReturnsLaterErrorOnNextRead(t *testing.T) {
	failure := errors.New("next chunk failed")
	calls := 0
	read := func(_ *opendalReader, p []byte, _ *opendalCancelToken) (uint, error) {
		calls++
		if calls == 1 {
			return uint(copy(p, "abc")), nil
		}
		return 0, failure
	}
	r := &Reader{ctx: context.WithValue(context.Background(), ffiReaderRead.sym, read), inner: newTestReaderHandle()}
	buf := make([]byte, 8)
	if n, err := r.Read(buf); n != 3 || err != nil {
		t.Fatalf("first Read = (%d, %v), want (3, nil)", n, err)
	}
	if n, err := r.Read(buf); n != 0 || err != failure {
		t.Fatalf("second Read = (%d, %v), want (0, failure)", n, err)
	}
}

func TestReaderShortReadsWithReadFullSeekAndClose(t *testing.T) {
	src := bytes.NewReader([]byte("abcdefgh"))
	read := func(_ *opendalReader, p []byte, _ *opendalCancelToken) (uint, error) {
		n, err := src.Read(p[:min(len(p), 3)])
		if err == io.EOF {
			err = nil
		}
		return uint(n), err
	}
	ctx := context.WithValue(context.Background(), ffiReaderRead.sym, read)
	ctx = context.WithValue(ctx, ffiReaderSeek.sym, func(_ *opendalReader, offset int64, whence int, _ *opendalCancelToken) (int64, error) {
		return src.Seek(offset, whence)
	})
	closed := 0
	ctx = context.WithValue(ctx, ffiReaderFree.sym, func(_ *opendalReader) { closed++ })
	r := &Reader{ctx: ctx, inner: newTestReaderHandle()}
	buf := make([]byte, 8)
	if n, err := r.Read(buf); n != 3 || err != nil {
		t.Fatalf("Read = (%d, %v)", n, err)
	}
	if pos, err := r.Seek(0, io.SeekCurrent); pos != 3 || err != nil {
		t.Fatalf("SeekCurrent = (%d, %v), want 3", pos, err)
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := io.ReadFull(r, buf); n != 8 || err != nil || string(buf) != "abcdefgh" {
		t.Fatalf("ReadFull = (%d, %v), data %q", n, err, buf)
	}
	if n, err := r.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("EOF = (%d, %v)", n, err)
	}
	if _, err := r.Seek(-2, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if n, err := io.ReadFull(r, buf); n != 2 || err != io.ErrUnexpectedEOF || string(buf[:n]) != "gh" {
		t.Fatalf("partial ReadFull = (%d, %v), data %q", n, err, buf[:n])
	}
	if closed != 0 {
		t.Fatal("reading or seeking closed the reader")
	}
	if err := r.Close(); err != nil || closed != 1 {
		t.Fatalf("Close = %v, calls = %d", err, closed)
	}
}

func TestReaderSeekRejectsInvalidWhenceAndPositionOverflow(t *testing.T) {
	called := false
	ctx := context.WithValue(context.Background(), ffiReaderSeek.sym, func(_ *opendalReader, _ int64, _ int, _ *opendalCancelToken) (int64, error) {
		called = true
		return 0, nil
	})
	r := &Reader{ctx: ctx, inner: newTestReaderHandle()}

	invalidWhences := []int{-1, 3}
	if strconv.IntSize >= 64 {
		invalidWhences = append(
			invalidWhences,
			int(int64(1)<<32),
			int((int64(1)<<32)+1),
			int((int64(1)<<32)+2),
		)
	}
	for _, whence := range invalidWhences {
		if _, err := r.Seek(0, whence); err == nil {
			t.Fatalf("Seek(0, %d) succeeded, want error", whence)
		}
		if called {
			t.Fatalf("Seek(0, %d) crossed FFI boundary", whence)
		}
	}

	// Verify ffiReaderSeek rejects invalid whence directly and guards pos > math.MaxInt64.
	cCalled := false
	seekOverflow := ffiReaderSeek.withFunc(context.Background(), func(_ *opendalReader, _ int64, _ int32, _ *opendalCancelToken, pos *uint64) *opendalError {
		cCalled = true
		*pos = uint64(math.MaxInt64) + 1
		return nil
	})
	if _, err := seekOverflow(newTestReaderHandle(), 0, -1, nil); err == nil {
		t.Fatal("ffiReaderSeek with invalid whence succeeded, want error")
	}
	if cCalled {
		t.Fatal("ffiReaderSeek invoked C symbol for invalid whence")
	}
	if _, err := seekOverflow(newTestReaderHandle(), 0, io.SeekStart, nil); err == nil {
		t.Fatal("ffiReaderSeek with pos > math.MaxInt64 succeeded, want error")
	}

	seekMaxInt64 := ffiReaderSeek.withFunc(context.Background(), func(_ *opendalReader, _ int64, _ int32, _ *opendalCancelToken, pos *uint64) *opendalError {
		*pos = uint64(math.MaxInt64)
		return nil
	})
	pos, err := seekMaxInt64(newTestReaderHandle(), 0, io.SeekStart, nil)
	if err != nil || pos != math.MaxInt64 {
		t.Fatalf("ffiReaderSeek(MaxInt64) = (%d, %v), want (%d, nil)", pos, err, int64(math.MaxInt64))
	}
}

func BenchmarkReaderProgressive(b *testing.B) {
	data := make([]byte, 8<<20)
	for _, size := range []int{32 << 10, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			pos := 0
			nativeReads := 0
			read := func(_ *opendalReader, p []byte, _ *opendalCancelToken) (uint, error) {
				nativeReads++
				n := copy(p[:min(len(p), 4096)], data[pos:])
				pos += n
				return uint(n), nil
			}
			r := &Reader{ctx: context.WithValue(context.Background(), ffiReaderRead.sym, read), inner: newTestReaderHandle()}
			buf := make([]byte, size)
			calls := 0
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				pos = 0
				for pos < len(data) {
					if _, err := r.Read(buf); err != nil {
						b.Fatal(err)
					}
					calls++
				}
			}
			b.ReportMetric(float64(calls)/float64(b.N), "go-reads/op")
			b.ReportMetric(float64(nativeReads)/float64(b.N), "native-reads/op")
		})
	}
}
