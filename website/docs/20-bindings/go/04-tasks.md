---
title: Common tasks
sidebar_label: Common tasks
description: Task-oriented recipes for OpenDAL in Go — read, write, stream, list, delete, copy, rename, and presign.
---

# Common tasks

Recipes for the things you actually do with storage. Each works on every
service. They assume an `op` built as in
[Getting started](./02-getting-started.md). For full signatures, see the
[API reference](https://pkg.go.dev/github.com/apache/opendal/bindings/go).

A few conventions used throughout:

- Every I/O method takes a `context.Context` as its first argument (`ctx` below).
- Paths are relative to the operator's root; a trailing `/` means a directory
  (`logs/app/` is a directory, `logs/app` is a file).
- `Read` returns `[]byte`; `Write` takes `[]byte`.
- Every operation returns an `error` as its last value — always check it.
- Many verbs accept functional options (`opendal.ReadWithRange(...)`,
  `opendal.WriteWithContentType(...)`, …) as trailing arguments.

## Read a whole file

```go
data, err := op.Read(ctx, "path/to/file")
if err != nil {
	log.Fatal(err)
}
text := string(data)
```

## Read part of a file

`ReadWithRange(offset, length)` reads `length` bytes starting at `offset`:

```go
data, err := op.Read(ctx, "path/to/file", opendal.ReadWithRange(0, 1024))
```

Use `opendal.ReadWithRangeFrom(offset)` to read from `offset` to the end.

## Stream a large file

Don't load gigabytes into memory — use a `Reader`, which implements
`io.ReadSeekCloser`, and read in chunks. Use `ReadContext` and `SeekContext`
for per-call context cancellation.

`Reader.Read` returns after one read from the storage library.
It can return fewer bytes than the buffer can hold, even before EOF.
Process `buf[:n]` before you check the returned error.
Use `io.ReadFull(r, buf)` if you need to fill the buffer.

```go
r, err := op.Reader(ctx, "big.bin")
if err != nil {
	log.Fatal(err)
}
defer r.Close()

buf := make([]byte, 8192)
for {
	n, err := r.ReadContext(ctx, buf)
	if n > 0 {
		// process buf[:n]
	}
	if err == io.EOF {
		break
	}
	if err != nil {
		log.Fatal(err)
	}
}
```

## Write a whole file

```go
_, err := op.Write(ctx, "path/to/file", []byte("Hello, World!"))
```

## Stream a large upload

Use a `Writer` to write data in chunks. It implements `io.Writer`.
Call `Write` (or `WriteContext` for per-call cancellation) for each chunk. Call
`Close(ctx)` to complete the write and get the object's metadata.

```go
w, err := op.Writer(ctx, "big.bin")
if err != nil {
	log.Fatal(err)
}
if _, err := w.WriteContext(ctx, firstChunk); err != nil {
	log.Fatal(err)
}
if _, err := w.WriteContext(ctx, secondChunk); err != nil {
	log.Fatal(err)
}
if _, err := w.Close(ctx); err != nil {
	log.Fatal(err)
}
```

## Copy between streams

Use `io.Copy(w, src)` to upload from an `io.Reader`, such as an open local file.
Use `io.Copy(dst, r)` to download to an `io.Writer`.

When Go selects OpenDAL's `Writer.ReadFrom` or `Reader.WriteTo`, the method
allocates one 256 KiB buffer and reuses it until EOF or an error. It returns the
number of bytes written, including any bytes written before an error.

Both methods leave the streams open. Call `w.Close(ctx)` to complete the write
and get the object's metadata. Check both the copy and close errors. Close the
reader after use.

`io.CopyBuffer` also uses the method's own buffer when it selects either
OpenDAL copy method.

## Upload concurrently

For large objects on services with multipart support, upload parts in parallel:

```go
_, err := op.Write(ctx, "big.bin", data, opendal.WriteWithConcurrent(8))
```

## Check existence and metadata

```go
exists, err := op.IsExist(ctx, "path/to/file")
if err != nil {
	log.Fatal(err)
}
if exists {
	meta, err := op.Stat(ctx, "path/to/file")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d bytes, dir = %v\n", meta.ContentLength(), meta.IsDir())
}
```

## List a directory

`List` returns a `Lister` you iterate with `Next` and `Entry`. Always check
`Error` after the loop:

```go
lister, err := op.List(ctx, "dir/")
if err != nil {
	log.Fatal(err)
}
defer lister.Close()

for lister.Next(ctx) {
	entry := lister.Entry()
	fmt.Println(entry.Path())
}
if err := lister.Error(); err != nil {
	log.Fatal(err)
}
```

## Walk a tree recursively

```go
lister, err := op.List(ctx, "dir/", opendal.ListWithRecursive(true))
```

The iteration is identical; only the option changes.

## Delete a file or a whole tree

```go
err := op.Delete(ctx, "path/to/file")
err = op.Delete(ctx, "a/", opendal.DeleteWithRecursive(true))
```

`Delete` succeeds even if the path does not exist.

Pass a trailing slash to limit recursive deletion to the directory. On
prefix-based services, `"a"` can also match entries under `"ab/"`.

## Delete many paths

```go
err := op.Remove(ctx, []string{"a.txt", "b.txt", "c.txt"})
```

`Remove` uses batch deletion where the service supports it. It submits the whole
slice to the C library in one call. Submission stops at the first error, and
earlier paths may already have been deleted.

Use `WithDeleter` to submit paths as you discover them or to set per-path options.
It flushes pending deletions when your function returns nil and releases the
handle on every exit, including a panic.

```go
err := op.WithDeleter(ctx, func(d *opendal.Deleter) error {
	for _, path := range paths {
		if err := d.Delete(ctx, path); err != nil {
			return err
		}
	}
	return nil
})
```

If the function returns an error or panics, `WithDeleter` releases the handle
without flushing. `Delete` can complete deletions immediately or flush a full
batch, so some paths may already be gone.

`Remove` uses default options for every path. On a service that supports
versioned deletion, specify a version with `Deleter.Delete`.

```go
err := op.WithDeleter(ctx, func(d *opendal.Deleter) error {
	return d.Delete(ctx, "a.txt", opendal.DeleteWithVersion("v1"))
})
```

Use `Deleter` directly when you need to retry a failed batch. `Flush` leaves the
handle open for retries. Always release the handle with `Close` or `Discard`.

```go
func deleteAsDiscovered(ctx context.Context, op *opendal.Operator, paths []string) error {
	deleter, err := op.Deleter(ctx)
	if err != nil {
		return err
	}
	// Release the handle if a submission or flush fails.
	defer deleter.Discard()

	for _, path := range paths {
		if err := deleter.Delete(ctx, path); err != nil {
			return err
		}
	}
	// A failed batch flush keeps pending paths queued for retry.
	var flushErr error
	for range 3 {
		if flushErr = deleter.Flush(ctx); flushErr == nil {
			break
		}
	}
	if flushErr != nil {
		return flushErr
	}
	// Close flushes pending entries and releases the handle.
	return deleter.Close(ctx)
}
```

## Create a directory

```go
err := op.CreateDir(ctx, "path/to/dir/") // the trailing slash is required
```

Creating a directory that already exists succeeds, and creation is recursive
(like `mkdir -p`).

## Copy and rename

```go
err := op.Copy(ctx, "from.txt", "to.txt")
err = op.Rename(ctx, "old.txt", "new.txt")
```

Both operate within a single operator and require a service that supports them;
see [capability checks](./05-production.md#capability-checks).

## Generate a presigned URL

Hand a time-limited URL to a third party so they can access an object without
your credentials. The presign methods return a standard `*net/http.Request`:

```go
import "time"

req, err := op.PresignRead(ctx, "path/to/file", time.Hour)
if err != nil {
	log.Fatal(err)
}
fmt.Println(req.Method, req.URL)
fmt.Println(req.Header)
```

`PresignWrite`, `PresignStat`, and `PresignDelete` work the same way for the
other operations.
