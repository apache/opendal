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
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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

func TestLiveCDeleterStopsAtFirstError(t *testing.T) {
	ctx := context.Background()
	op, err := NewOperator(localTestScheme{name: "fs", path: findLocalCLibrary(t)}, OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer op.Close()

	paths := []string{"first.txt", "blocked.txt", "last.txt"}
	for _, path := range paths {
		if _, err := op.Write(ctx, path, []byte("test")); err != nil {
			t.Fatalf("Write(%q): %v", path, err)
		}
	}

	// FS deletes each path immediately and rejects a directory path to a file.
	err = op.Remove(ctx, []string{paths[0], paths[1] + "/", paths[2]})
	var opErr *Error
	if !errors.As(err, &opErr) || opErr.Code() != CodeNotADirectory {
		t.Fatalf("Remove error = %v, want CodeNotADirectory", err)
	}
	for i, path := range paths {
		want := i != 0
		if exists, err := op.IsExist(ctx, path); err != nil || exists != want {
			t.Fatalf("IsExist(%q) = %v, %v, want %v, nil", path, exists, err, want)
		}
	}
}

func TestLiveCLibraryEndToEndAndNoLibffi(t *testing.T) {
	ctx := context.Background()
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

	if err := op.Check(ctx); err != nil {
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
	exists, err := op.IsExist(ctx, "missing.txt")
	if err != nil {
		t.Fatalf("IsExist(missing.txt) failed: %v", err)
	}
	if exists {
		t.Fatal("IsExist(missing.txt) = true, want false")
	}

	if _, err := op.Read(ctx, "missing.txt"); err == nil {
		t.Fatal("Read(missing.txt) succeeded, want error")
	} else {
		var odErr *Error
		if !errors.As(err, &odErr) || odErr.Code() != CodeNotFound {
			t.Fatalf("Read(missing.txt) error = %v, want CodeNotFound", err)
		}
	}

	payload := []byte("hello purego c abi output parameters")
	writeMeta, err := op.Write(ctx, "dir/hello.txt", payload, WriteWithContentType("text/plain"))
	if err != nil {
		t.Fatalf("Write(dir/hello.txt) failed: %v", err)
	}
	if writeMeta == nil {
		t.Fatal("Write(dir/hello.txt) returned nil metadata")
	}

	exists, err = op.IsExist(ctx, "dir/hello.txt")
	if err != nil || !exists {
		t.Fatalf("IsExist(dir/hello.txt) = %v, %v, want true, nil", exists, err)
	}

	statMeta, err := op.Stat(ctx, "dir/hello.txt")
	if err != nil {
		t.Fatalf("Stat(dir/hello.txt) failed: %v", err)
	}
	if statMeta.ContentLength() != uint64(len(payload)) || !statMeta.IsFile() {
		t.Fatalf("Stat(dir/hello.txt) len=%d isFile=%v, want %d true",
			statMeta.ContentLength(), statMeta.IsFile(), len(payload))
	}

	got, err := op.Read(ctx, "dir/hello.txt")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Read(dir/hello.txt) = %q, %v, want %q, nil", got, err, payload)
	}

	rangeGot, err := op.Read(ctx, "dir/hello.txt", ReadWithRange(6, 6))
	if err != nil || string(rangeGot) != "purego" {
		t.Fatalf("ReadWithRange(6, 6) = %q, %v, want purego, nil", rangeGot, err)
	}

	rangeFromGot, err := op.Read(ctx, "dir/hello.txt", ReadWithRangeFrom(19))
	if err != nil || string(rangeFromGot) != "output parameters" {
		t.Fatalf("ReadWithRangeFrom(19) = %q, %v, want 'output parameters', nil", rangeFromGot, err)
	}

	// Empty file write and read.
	if _, err := op.Write(ctx, "dir/empty.txt", []byte{}); err != nil {
		t.Fatalf("Write(dir/empty.txt) failed: %v", err)
	}
	emptyGot, err := op.Read(ctx, "dir/empty.txt")
	if err != nil || len(emptyGot) != 0 {
		t.Fatalf("Read(dir/empty.txt) = %q (len=%d), %v, want empty, nil", emptyGot, len(emptyGot), err)
	}

	// 5. Streaming Writer and Reader (Read, Seek, EOF).
	w, err := op.Writer(ctx, "dir/stream.txt", WriteWithChunk(8))
	if err != nil {
		t.Fatalf("Writer(dir/stream.txt) failed: %v", err)
	}
	streamData := []byte("0123456789abcdef")
	n, err := w.Write(streamData[:10])
	if err != nil || n != 10 {
		t.Fatalf("w.Write first chunk = %d, %v, want 10, nil", n, err)
	}
	n, err = w.WriteContext(ctx, streamData[10:])
	if err != nil || n != 6 {
		t.Fatalf("w.WriteContext second chunk = %d, %v, want 6, nil", n, err)
	}
	closeMeta, err := w.Close(ctx)
	if err != nil || closeMeta == nil {
		t.Fatalf("w.Close() = %v, %v, want non-nil metadata, nil", closeMeta, err)
	}

	r, err := op.Reader(ctx, "dir/stream.txt", ReaderWithChunk(4))
	if err != nil {
		t.Fatalf("Reader(dir/stream.txt) failed: %v", err)
	}
	smallBuf := make([]byte, 5)
	n, err = io.ReadFull(r, smallBuf)
	if err != nil || n != 5 || string(smallBuf) != "01234" {
		t.Fatalf("io.ReadFull(5) = %d (%q), %v, want 5 (01234), nil", n, smallBuf[:n], err)
	}
	pos, err := r.SeekContext(ctx, 10, io.SeekStart)
	if err != nil || pos != 10 {
		t.Fatalf("r.SeekContext(10, SeekStart) = %d, %v, want 10, nil", pos, err)
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
	lister, err := op.List(ctx, "dir/", ListWithRecursive(true))
	if err != nil {
		t.Fatalf("List(dir/) failed: %v", err)
	}
	var listedPaths []string
	for lister.Next(ctx) {
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

	if err := op.CreateDir(ctx, "emptydir/"); err != nil {
		t.Fatalf("CreateDir(emptydir/) failed: %v", err)
	}
	emptyLister, err := op.List(ctx, "emptydir/")
	if err != nil {
		t.Fatalf("List(emptydir/) failed: %v", err)
	}
	for emptyLister.Next(ctx) {
		if e := emptyLister.Entry(); e != nil && e.Path() != "emptydir/" {
			t.Fatalf("unexpected entry in emptydir/: %q", e.Path())
		}
	}
	if err := emptyLister.Error(); err != nil {
		t.Fatalf("emptyLister.Error() = %v, want nil", err)
	}
	_ = emptyLister.Close()

	// 7. Delete on memory, and unsupported Copy/Rename/Presign/Copier on memory return errors cleanly.
	if err := op.Copy(ctx, "dir/hello.txt", "dir/hello_copy.txt"); err == nil {
		t.Fatal("Copy on memory operator succeeded, want unsupported error")
	}
	if err := op.Rename(ctx, "dir/hello.txt", "dir/hello_moved.txt"); err == nil {
		t.Fatal("Rename on memory operator succeeded, want unsupported error")
	}
	if _, err := op.PresignRead(ctx, "dir/hello.txt", time.Minute); err == nil {
		t.Fatal("PresignRead on memory operator succeeded, want unsupported error")
	}
	if _, err := op.Copier(ctx, "dir/hello.txt", "dir/hello_copier.txt"); err == nil {
		t.Fatal("Copier on memory operator succeeded, want unsupported error")
	}
	if err := op.Delete(ctx, "dir/empty.txt"); err != nil {
		t.Fatalf("Delete(dir/empty.txt) failed: %v", err)
	}

	// 8. FS operator Copy, Rename, Delete, and Copier streaming completion test.
	tmpDir := t.TempDir()
	fsOp, err := NewOperator(localTestScheme{name: "fs", path: libPath}, OperatorOptions{"root": tmpDir})
	if err != nil {
		t.Fatalf("failed to create fs operator: %v", err)
	}
	defer fsOp.Close()

	if _, err := fsOp.Write(ctx, "src.txt", payload); err != nil {
		t.Fatalf("fsOp.Write(src.txt) failed: %v", err)
	}
	if err := fsOp.Copy(ctx, "src.txt", "copy.txt"); err != nil {
		t.Fatalf("fsOp.Copy failed: %v", err)
	}
	if err := fsOp.Rename(ctx, "copy.txt", "moved.txt"); err != nil {
		t.Fatalf("fsOp.Rename failed: %v", err)
	}
	movedGot, err := fsOp.Read(ctx, "moved.txt")
	if err != nil || !bytes.Equal(movedGot, payload) {
		t.Fatalf("fsOp.Read(moved.txt) = %q, %v", movedGot, err)
	}
	if err := fsOp.Delete(ctx, "moved.txt"); err != nil {
		t.Fatalf("fsOp.Delete(moved.txt) failed: %v", err)
	}

	copier, err := fsOp.Copier(ctx, "src.txt", "dst.txt")
	if err != nil {
		t.Fatalf("fsOp.Copier(src.txt, dst.txt) failed: %v", err)
	}
	for {
		_, ok, err := copier.Next(ctx)
		if err != nil {
			t.Fatalf("copier.Next() failed: %v", err)
		}
		if !ok {
			break
		}
	}
	if n, ok, err := copier.Next(ctx); err != nil || ok || n != 0 {
		t.Fatalf("copier.Next() after completion = (%d, %v, %v), want (0, false, nil)", n, ok, err)
	}
	if err := copier.Close(); err != nil {
		t.Fatalf("copier.Close() failed: %v", err)
	}
	if err := copier.Close(); err != nil {
		t.Fatalf("second copier.Close() failed: %v", err)
	}
	dstGot, err := fsOp.Read(ctx, "dst.txt")
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

func TestLiveCFFIValidationAndInvariants(t *testing.T) {
	ctx := context.Background()
	libPath := findLocalCLibrary(t)

	// Non-UTF-8 strings in operator options must fail in Go rather than aborting inside bindings/c.
	if _, err := NewOperator(localTestScheme{name: "memory", path: libPath}, OperatorOptions{"root": "\xff"}); err == nil {
		t.Fatal("NewOperator with invalid UTF-8 option value succeeded, want error")
	}
	if _, err := NewOperator(localTestScheme{name: "memory", path: libPath}, OperatorOptions{"\xff": "/tmp"}); err == nil {
		t.Fatal("NewOperator with invalid UTF-8 option key succeeded, want error")
	}
	if _, err := NewOperator(localTestScheme{name: "\xff", path: libPath}, nil); err == nil {
		t.Fatal("NewOperator with invalid UTF-8 scheme succeeded, want error")
	}

	// RetryFactor overflow past math.MaxFloat32 is rejected before crossing FFI.
	if _, err := NewOperator(localTestScheme{name: "memory", path: libPath}, nil, WithRetry(RetryFactor(math.MaxFloat64))); err == nil {
		t.Fatal("NewOperator with RetryFactor(MaxFloat64) succeeded, want error")
	}

	memOp, err := NewOperator(localTestScheme{name: "memory", path: libPath}, OperatorOptions{"root": "/validation"})
	if err != nil {
		t.Fatalf("NewOperator(memory): %v", err)
	}
	defer memOp.Close()

	if _, err := memOp.Write(ctx, "dir/a.txt", []byte("0123456789")); err != nil {
		t.Fatalf("Write(dir/a.txt): %v", err)
	}
	if _, err := memOp.Write(ctx, "dir/b.txt", []byte("abcdefghij")); err != nil {
		t.Fatalf("Write(dir/b.txt): %v", err)
	}

	// Non-UTF-8 paths/options and invalid ListWithStartAfter return Go errors without aborting.
	nonUTF8Checks := map[string]func() error{
		"Read":      func() error { _, err := memOp.Read(ctx, "\xff"); return err },
		"Stat":      func() error { _, err := memOp.Stat(ctx, "\xff"); return err },
		"IsExist":   func() error { _, err := memOp.IsExist(ctx, "\xff"); return err },
		"WritePath": func() error { _, err := memOp.Write(ctx, "\xff", []byte("x")); return err },
		"WriteOpt": func() error {
			_, err := memOp.Write(ctx, "a.txt", []byte("x"), WriteWithContentType("\xff"))
			return err
		},
		"WriteMeta": func() error {
			_, err := memOp.Write(ctx, "a.txt", []byte("x"), WriteWithUserMetadata(map[string]string{"k": "\xff"}))
			return err
		},
		"ListPath":  func() error { _, err := memOp.List(ctx, "\xff"); return err },
		"ListStart": func() error { _, err := memOp.List(ctx, "dir/", ListWithStartAfter("\xff")); return err },
		"ListNul":   func() error { _, err := memOp.List(ctx, "dir/", ListWithStartAfter("a\x00b")); return err },
		"Delete":    func() error { return memOp.Delete(ctx, "\xff") },
		"Remove":    func() error { return memOp.Remove(ctx, []string{"\xff"}) },
		"CreateDir": func() error { return memOp.CreateDir(ctx, "\xff/") },
		"Rename":    func() error { return memOp.Rename(ctx, "\xff", "b.txt") },
		"Copy":      func() error { return memOp.Copy(ctx, "\xff", "b.txt") },
		"Presign":   func() error { _, err := memOp.PresignRead(ctx, "\xff", time.Minute); return err },
	}
	for name, check := range nonUTF8Checks {
		if err := check(); err == nil {
			t.Fatalf("%s with invalid string succeeded, want error", name)
		}
	}

	// Invalid presign durations return a Go validation error rather than crossing FFI.
	for _, expire := range []time.Duration{-time.Second, 500 * time.Millisecond} {
		for name, call := range map[string]func() error{
			"PresignRead":   func() error { _, err := memOp.PresignRead(ctx, "dir/a.txt", expire); return err },
			"PresignWrite":  func() error { _, err := memOp.PresignWrite(ctx, "dir/a.txt", expire); return err },
			"PresignDelete": func() error { _, err := memOp.PresignDelete(ctx, "dir/a.txt", expire); return err },
			"PresignStat":   func() error { _, err := memOp.PresignStat(ctx, "dir/a.txt", expire); return err },
		} {
			err := call()
			if err == nil {
				t.Fatalf("%s(%v) succeeded, want validation error", name, expire)
			}
			var opErr *Error
			if errors.As(err, &opErr) {
				t.Fatalf("%s(%v) crossed FFI and returned native error %v", name, expire, err)
			}
		}
	}

	// Reader.SeekContext rejects invalid whence values (including 64-bit truncation) cleanly.
	reader, err := memOp.Reader(ctx, "dir/a.txt")
	if err != nil {
		t.Fatalf("Reader(dir/a.txt): %v", err)
	}
	defer func() { _ = reader.Close() }()
	if _, err := reader.SeekContext(ctx, 4, io.SeekStart); err != nil {
		t.Fatalf("SeekContext(4, SeekStart): %v", err)
	}
	if _, err := reader.SeekContext(ctx, 0, -1); err == nil {
		t.Fatal("SeekContext(0, -1) succeeded, want error")
	}
	if strconv.IntSize >= 64 {
		if _, err := reader.SeekContext(ctx, 0, int(int64(1)<<32)); err == nil {
			t.Fatal("SeekContext(0, 1<<32) succeeded, want error")
		}
	}
	// Position must remain unchanged at 4 after rejected seeks.
	pos, err := reader.SeekContext(ctx, 0, io.SeekCurrent)
	if err != nil || pos != 4 {
		t.Fatalf("SeekContext(0, SeekCurrent) after rejected seeks = (%d, %v), want (4, nil)", pos, err)
	}

	// Lister.Next clears a prior error after a subsequent successful step.
	lister, err := memOp.List(ctx, "dir/")
	if err != nil {
		t.Fatalf("List(dir/): %v", err)
	}
	defer func() { _ = lister.Close() }()
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if lister.Next(canceledCtx) {
		t.Fatal("lister.Next(canceledCtx) = true, want false")
	}
	if lister.Error() == nil {
		t.Fatal("lister.Error() after canceled Next = nil, want error")
	}
	if !lister.Next(ctx) {
		t.Fatalf("lister.Next(ctx) after retry = false (err=%v), want true", lister.Error())
	}
	if lister.Error() != nil {
		t.Fatalf("lister.Error() after successful retry = %v, want nil", lister.Error())
	}

	// Live fs operator verifies LastModified UTC, CopyWithSourceContentLengthHint(0), and nil options.
	fsOp, err := NewOperator(localTestScheme{name: "fs", path: libPath}, OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatalf("NewOperator(fs): %v", err)
	}
	defer fsOp.Close()

	if _, err := fsOp.Write(ctx, "empty.txt", nil, nil); err != nil {
		t.Fatalf("fsOp.Write with nil option: %v", err)
	}
	if err := fsOp.Copy(ctx, "empty.txt", "empty-copy.txt", nil, CopyWithSourceContentLengthHint(0), nil); err != nil {
		t.Fatalf("fsOp.Copy with CopyWithSourceContentLengthHint(0) and nil option: %v", err)
	}
	if data, err := fsOp.Read(ctx, "empty-copy.txt", nil); err != nil || len(data) != 0 {
		t.Fatalf("fsOp.Read with nil option = (%q, %v), want empty, nil", data, err)
	}
	rNilOpt, err := fsOp.Reader(ctx, "empty-copy.txt", nil)
	if err != nil {
		t.Fatalf("fsOp.Reader with nil option: %v", err)
	}
	_ = rNilOpt.Close()
	lNilOpt, err := fsOp.List(ctx, "", nil)
	if err != nil {
		t.Fatalf("fsOp.List with nil option: %v", err)
	}
	_ = lNilOpt.Close()

	origLocal := time.Local
	time.Local = time.FixedZone("CST", 8*3600)
	meta, statErr := fsOp.Stat(ctx, "empty.txt", nil)
	time.Local = origLocal
	if statErr != nil {
		t.Fatalf("fsOp.Stat with nil option: %v", statErr)
	}
	if meta.LastModified().IsZero() {
		t.Fatal("fsOp.Stat LastModified() is zero, expected non-zero timestamp")
	}
	if meta.LastModified().Location() != time.UTC {
		t.Fatalf("fsOp.Stat LastModified().Location() = %v, want UTC", meta.LastModified().Location())
	}
	if err := fsOp.Delete(ctx, "empty-copy.txt", nil); err != nil {
		t.Fatalf("fsOp.Delete with nil option: %v", err)
	}

	// Operator.Remove(ctx, nil) on a closed live operator returns ErrOperatorClosed.
	memOp.Close()
	if err := memOp.Remove(ctx, nil); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Remove(nil) on closed operator = %v, want ErrOperatorClosed", err)
	}
}
