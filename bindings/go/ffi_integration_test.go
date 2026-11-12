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
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func findLocalCLibrary(t *testing.T) string {
	t.Helper()

	if envPath := os.Getenv("OPENDAL_C_LIB_PATH"); envPath != "" {
		if _, err := os.Stat(envPath); err == nil {
			return envPath
		}
		t.Fatalf("OPENDAL_C_LIB_PATH=%q does not exist", envPath)
	}

	var libNames []string
	switch runtime.GOOS {
	case "darwin":
		libNames = []string{"libopendal_c.dylib"}
	case "windows":
		libNames = []string{"opendal_c.dll"}
	default:
		libNames = []string{"libopendal_c.so"}
	}

	for _, profile := range []string{"debug", "release"} {
		for _, name := range libNames {
			candidate := filepath.Join("..", "c", "target", profile, name)
			if abs, err := filepath.Abs(candidate); err == nil {
				if _, err := os.Stat(abs); err == nil {
					return abs
				}
			}
		}
	}

	t.Skip("local libopendal_c shared library not found; build bindings/c to run live C ABI tests")
	return ""
}

type localTestScheme struct {
	name string
	path string
}

func (s localTestScheme) Name() string    { return s.name }
func (s localTestScheme) Path() string    { return s.path }
func (s localTestScheme) LoadOnce() error { return nil }

func TestLiveCLibraryEndToEndAndNoLibffi(t *testing.T) {
	libPath := findLocalCLibrary(t)

	// 1. Operator creation failure (invalid scheme) returns error cleanly.
	if _, err := NewOperator(localTestScheme{name: "invalid-scheme-not-registered", path: libPath}, nil); err == nil {
		t.Fatal("expected error when creating operator with invalid scheme, got nil")
	}

	// 2. Memory operator creation with layers & options.
	op, err := NewOperator(
		localTestScheme{name: "memory", path: libPath},
		OperatorOptions{"root": "/testroot"},
		WithTimeout(5*time.Second, 5*time.Second),
		WithRetry(RetryJitter(), RetryMaxTimes(2)),
	)
	if err != nil {
		t.Fatalf("failed to create memory operator: %v", err)
	}
	defer op.Close()

	if err := op.Check(); err != nil {
		t.Fatalf("op.Check() failed: %v", err)
	}

	// 3. OperatorInfo and Capability via output parameter.
	info := op.Info()
	if info.GetScheme() != "memory" {
		t.Fatalf("info.GetScheme() = %q, want memory", info.GetScheme())
	}
	if info.GetRoot() != "/testroot/" {
		t.Fatalf("info.GetRoot() = %q, want /testroot/", info.GetRoot())
	}
	cap := info.GetCapability()
	if !cap.Stat() || !cap.Read() || !cap.Write() || !cap.WriteCanEmpty() || !cap.List() {
		t.Fatalf("unexpected memory capability: stat=%v read=%v write=%v writeCanEmpty=%v list=%v",
			cap.Stat(), cap.Read(), cap.Write(), cap.WriteCanEmpty(), cap.List())
	}

	// 4. Write, Read (non-empty, empty, range, not-found error), Stat, IsExist.
	exists, err := op.IsExist("missing.txt")
	if err != nil {
		t.Fatalf("IsExist(missing.txt) failed: %v", err)
	}
	if exists {
		t.Fatal("IsExist(missing.txt) = true, want false")
	}

	if _, err := op.Read("missing.txt"); err == nil {
		t.Fatal("Read(missing.txt) succeeded, want error")
	} else {
		var odErr *Error
		if !errors.As(err, &odErr) || odErr.Code() != CodeNotFound {
			t.Fatalf("Read(missing.txt) error = %v, want CodeNotFound", err)
		}
	}

	payload := []byte("hello purego c abi output parameters")
	writeMeta, err := op.Write("dir/hello.txt", payload, WriteWithContentType("text/plain"))
	if err != nil {
		t.Fatalf("Write(dir/hello.txt) failed: %v", err)
	}
	if writeMeta == nil {
		t.Fatal("Write(dir/hello.txt) returned nil metadata")
	}

	exists, err = op.IsExist("dir/hello.txt")
	if err != nil || !exists {
		t.Fatalf("IsExist(dir/hello.txt) = %v, %v, want true, nil", exists, err)
	}

	statMeta, err := op.Stat("dir/hello.txt")
	if err != nil {
		t.Fatalf("Stat(dir/hello.txt) failed: %v", err)
	}
	if statMeta.ContentLength() != uint64(len(payload)) || !statMeta.IsFile() {
		t.Fatalf("Stat(dir/hello.txt) len=%d isFile=%v, want %d true",
			statMeta.ContentLength(), statMeta.IsFile(), len(payload))
	}

	got, err := op.Read("dir/hello.txt")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Read(dir/hello.txt) = %q, %v, want %q, nil", got, err, payload)
	}

	rangeGot, err := op.Read("dir/hello.txt", ReadWithRange(6, 6))
	if err != nil || string(rangeGot) != "purego" {
		t.Fatalf("ReadWithRange(6, 6) = %q, %v, want purego, nil", rangeGot, err)
	}

	rangeFromGot, err := op.Read("dir/hello.txt", ReadWithRangeFrom(19))
	if err != nil || string(rangeFromGot) != "output parameters" {
		t.Fatalf("ReadWithRangeFrom(19) = %q, %v, want 'output parameters', nil", rangeFromGot, err)
	}

	// Empty file write and read.
	if _, err := op.Write("dir/empty.txt", []byte{}); err != nil {
		t.Fatalf("Write(dir/empty.txt) failed: %v", err)
	}
	emptyGot, err := op.Read("dir/empty.txt")
	if err != nil || len(emptyGot) != 0 {
		t.Fatalf("Read(dir/empty.txt) = %q (len=%d), %v, want empty, nil", emptyGot, len(emptyGot), err)
	}

	// 5. Streaming Writer and Reader (Read, Seek, EOF).
	w, err := op.Writer("dir/stream.txt", WriteWithChunk(8))
	if err != nil {
		t.Fatalf("Writer(dir/stream.txt) failed: %v", err)
	}
	streamData := []byte("0123456789abcdef")
	n, err := w.Write(streamData[:10])
	if err != nil || n != 10 {
		t.Fatalf("w.Write first chunk = %d, %v, want 10, nil", n, err)
	}
	n, err = w.Write(streamData[10:])
	if err != nil || n != 6 {
		t.Fatalf("w.Write second chunk = %d, %v, want 6, nil", n, err)
	}
	closeMeta, err := w.Close()
	if err != nil || closeMeta == nil {
		t.Fatalf("w.Close() = %v, %v, want non-nil metadata, nil", closeMeta, err)
	}

	r, err := op.Reader("dir/stream.txt", ReaderWithChunk(4))
	if err != nil {
		t.Fatalf("Reader(dir/stream.txt) failed: %v", err)
	}
	smallBuf := make([]byte, 5)
	n, err = r.Read(smallBuf)
	if err != nil || n != 5 || string(smallBuf) != "01234" {
		t.Fatalf("r.Read(5) = %d (%q), %v, want 5 (01234), nil", n, smallBuf[:n], err)
	}
	pos, err := r.Seek(10, io.SeekStart)
	if err != nil || pos != 10 {
		t.Fatalf("r.Seek(10, SeekStart) = %d, %v, want 10, nil", pos, err)
	}
	rest, err := io.ReadAll(r)
	if err != nil || string(rest) != "abcdef" {
		t.Fatalf("io.ReadAll after seek = %q, %v, want abcdef, nil", rest, err)
	}
	// At EOF, another Read returns 0, io.EOF.
	n, err = r.Read(smallBuf)
	if err != io.EOF || n != 0 {
		t.Fatalf("r.Read at EOF = %d, %v, want 0, io.EOF", n, err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("r.Close() failed: %v", err)
	}

	// 6. Lister: non-empty listing, entry metadata, EOF, and empty directory listing.
	lister, err := op.List("dir/", ListWithRecursive(true))
	if err != nil {
		t.Fatalf("List(dir/) failed: %v", err)
	}
	var listedPaths []string
	for lister.Next() {
		entry := lister.Entry()
		if entry == nil {
			t.Fatal("lister.Entry() returned nil while Next() was true")
		}
		listedPaths = append(listedPaths, entry.Path())
	}
	if err := lister.Error(); err != nil {
		t.Fatalf("lister.Error() = %v, want nil", err)
	}
	if err := lister.Close(); err != nil {
		t.Fatalf("lister.Close() failed: %v", err)
	}
	if len(listedPaths) < 3 {
		t.Fatalf("listedPaths = %v, expected at least 3 entries", listedPaths)
	}

	if err := op.CreateDir("emptydir/"); err != nil {
		t.Fatalf("CreateDir(emptydir/) failed: %v", err)
	}
	emptyLister, err := op.List("emptydir/")
	if err != nil {
		t.Fatalf("List(emptydir/) failed: %v", err)
	}
	for emptyLister.Next() {
		if e := emptyLister.Entry(); e != nil && e.Path() != "emptydir/" {
			t.Fatalf("unexpected entry in emptydir/: %q", e.Path())
		}
	}
	if err := emptyLister.Error(); err != nil {
		t.Fatalf("emptyLister.Error() = %v, want nil", err)
	}
	_ = emptyLister.Close()

	// 7. Delete on memory, and unsupported Copy/Rename/Presign/Copier on memory return errors cleanly.
	if err := op.Copy("dir/hello.txt", "dir/hello_copy.txt"); err == nil {
		t.Fatal("Copy on memory operator succeeded, want unsupported error")
	}
	if err := op.Rename("dir/hello.txt", "dir/hello_moved.txt"); err == nil {
		t.Fatal("Rename on memory operator succeeded, want unsupported error")
	}
	if _, err := op.PresignRead("dir/hello.txt", time.Minute); err == nil {
		t.Fatal("PresignRead on memory operator succeeded, want unsupported error")
	}
	if _, err := op.Copier("dir/hello.txt", "dir/hello_copier.txt"); err == nil {
		t.Fatal("Copier on memory operator succeeded, want unsupported error")
	}
	if err := op.Delete("dir/empty.txt"); err != nil {
		t.Fatalf("Delete(dir/empty.txt) failed: %v", err)
	}

	// 8. FS operator Copy, Rename, Delete, and Copier streaming completion test.
	tmpDir := t.TempDir()
	fsOp, err := NewOperator(localTestScheme{name: "fs", path: libPath}, OperatorOptions{"root": tmpDir})
	if err != nil {
		t.Fatalf("failed to create fs operator: %v", err)
	}
	defer fsOp.Close()

	if _, err := fsOp.Write("src.txt", payload); err != nil {
		t.Fatalf("fsOp.Write(src.txt) failed: %v", err)
	}
	if err := fsOp.Copy("src.txt", "copy.txt"); err != nil {
		t.Fatalf("fsOp.Copy failed: %v", err)
	}
	if err := fsOp.Rename("copy.txt", "moved.txt"); err != nil {
		t.Fatalf("fsOp.Rename failed: %v", err)
	}
	movedGot, err := fsOp.Read("moved.txt")
	if err != nil || !bytes.Equal(movedGot, payload) {
		t.Fatalf("fsOp.Read(moved.txt) = %q, %v", movedGot, err)
	}
	if err := fsOp.Delete("moved.txt"); err != nil {
		t.Fatalf("fsOp.Delete(moved.txt) failed: %v", err)
	}

	copier, err := fsOp.Copier("src.txt", "dst.txt")
	if err != nil {
		t.Fatalf("fsOp.Copier(src.txt, dst.txt) failed: %v", err)
	}
	for {
		_, ok, err := copier.Next()
		if err != nil {
			t.Fatalf("copier.Next() failed: %v", err)
		}
		if !ok {
			break
		}
	}
	if n, ok, err := copier.Next(); err != nil || ok || n != 0 {
		t.Fatalf("copier.Next() after completion = (%d, %v, %v), want (0, false, nil)", n, ok, err)
	}
	if err := copier.Close(); err != nil {
		t.Fatalf("copier.Close() failed: %v", err)
	}
	if err := copier.Close(); err != nil {
		t.Fatalf("second copier.Close() failed: %v", err)
	}
	dstGot, err := fsOp.Read("dst.txt")
	if err != nil || !bytes.Equal(dstGot, payload) {
		t.Fatalf("fsOp.Read(dst.txt) = %q, %v", dstGot, err)
	}

	// 10. On Linux, verify /proc/self/maps contains libopendal_c and does NOT contain libffi.
	if runtime.GOOS == "linux" {
		mapsData, err := os.ReadFile("/proc/self/maps")
		if err != nil {
			t.Fatalf("failed to read /proc/self/maps: %v", err)
		}
		mapsText := string(mapsData)
		if !strings.Contains(mapsText, "libopendal_c") {
			t.Fatalf("/proc/self/maps did not contain libopendal_c")
		}
		if strings.Contains(mapsText, "libffi") {
			t.Fatalf("/proc/self/maps unexpectedly contained libffi:\n%s", mapsText)
		}
	}
}
