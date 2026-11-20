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

- Paths are relative to the operator's root; a trailing `/` means a directory
  (`logs/app/` is a directory, `logs/app` is a file).
- `Read` returns `[]byte`; `Write` takes `[]byte`.
- Every operation returns an `error` as its last value — always check it.
- Many verbs accept functional options (`opendal.ReadWithRange(...)`,
  `opendal.WriteWithContentType(...)`, …) as trailing arguments.

## Read a whole file

```go
data, err := op.Read("path/to/file")
if err != nil {
	log.Fatal(err)
}
text := string(data)
```

## Read part of a file

`ReadWithRange(offset, length)` reads `length` bytes starting at `offset`:

```go
data, err := op.Read("path/to/file", opendal.ReadWithRange(0, 1024))
```

Use `opendal.ReadWithRangeFrom(offset)` to read from `offset` to the end.

## Stream a large file

Don't load gigabytes into memory — use a `Reader`, which implements
`io.ReadSeekCloser`, and read in chunks:

```go
r, err := op.Reader("big.bin")
if err != nil {
	log.Fatal(err)
}
defer r.Close()

buf := make([]byte, 8192)
for {
	n, err := r.Read(buf)
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
err := op.Write("path/to/file", []byte("Hello, World!"))
```

## Stream a large upload

Use a `Writer` for data produced incrementally. It implements `io.WriteCloser`;
call `Write` repeatedly, then `Close` to commit. Each `Write` call accepts up to
256KB.

```go
w, err := op.Writer("big.bin")
if err != nil {
	log.Fatal(err)
}
if _, err := w.Write(firstChunk); err != nil {
	log.Fatal(err)
}
if _, err := w.Write(secondChunk); err != nil {
	log.Fatal(err)
}
// Close finishes the write; data may be lost if you skip it.
if err := w.Close(); err != nil {
	log.Fatal(err)
}
```

## Upload concurrently

For large objects on services with multipart support, upload parts in parallel:

```go
err := op.Write("big.bin", data, opendal.WriteWithConcurrent(8))
```

## Check existence and metadata

```go
exists, err := op.IsExist("path/to/file")
if err != nil {
	log.Fatal(err)
}
if exists {
	meta, err := op.Stat("path/to/file")
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
lister, err := op.List("dir/")
if err != nil {
	log.Fatal(err)
}
defer lister.Close()

for lister.Next() {
	entry := lister.Entry()
	fmt.Println(entry.Path())
}
if err := lister.Error(); err != nil {
	log.Fatal(err)
}
```

## Walk a tree recursively

```go
lister, err := op.List("dir/", opendal.ListWithRecursive(true))
```

The iteration is identical; only the option changes.

## Delete a file or a whole tree

```go
err := op.Delete("path/to/file")
err = op.Delete("a/", opendal.DeleteWithRecursive(true))
```

`Delete` succeeds even if the path does not exist.

Pass a trailing slash to limit recursive deletion to the directory. On
prefix-based services, `"a"` can also match entries under `"ab/"`.

## Delete many paths

```go
err := op.Remove([]string{"a.txt", "b.txt", "c.txt"})
```

`Remove` uses batch deletion where the service supports it. It submits the whole
slice to the C library in one call. Submission stops at the first error, and
earlier paths may already have been deleted.

Use `WithDeleter` to submit paths as you discover them or to set per-path options.
It flushes pending deletions when your function returns nil and releases the
handle on every exit, including a panic.

```go
err := op.WithDeleter(func(d *opendal.Deleter) error {
	for _, path := range paths {
		if err := d.Delete(path); err != nil {
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
err := op.WithDeleter(func(d *opendal.Deleter) error {
	return d.Delete("a.txt", opendal.DeleteWithVersion("v1"))
})
```

Use `Deleter` directly when you need to retry a failed batch. `Flush` leaves the
handle open for retries. Always release the handle with `Close` or `Discard`.

```go
func deleteAsDiscovered(op *opendal.Operator, paths []string) error {
	deleter, err := op.Deleter()
	if err != nil {
		return err
	}
	// Release the handle if a submission or flush fails.
	defer deleter.Discard()

	for _, path := range paths {
		if err := deleter.Delete(path); err != nil {
			return err
		}
	}
	// A failed batch flush keeps pending paths queued for retry.
	var flushErr error
	for range 3 {
		if flushErr = deleter.Flush(); flushErr == nil {
			break
		}
	}
	if flushErr != nil {
		return flushErr
	}
	// Close flushes pending entries and releases the handle.
	return deleter.Close()
}
```

## Create a directory

```go
err := op.CreateDir("path/to/dir/") // the trailing slash is required
```

Creating a directory that already exists succeeds, and creation is recursive
(like `mkdir -p`).

## Copy and rename

```go
err := op.Copy("from.txt", "to.txt")
err = op.Rename("old.txt", "new.txt")
```

Both operate within a single operator and require a service that supports them;
see [capability checks](./05-production.md#capability-checks).

## Generate a presigned URL

Hand a time-limited URL to a third party so they can access an object without
your credentials. The presign methods return a standard `*net/http.Request`:

```go
import "time"

req, err := op.PresignRead("path/to/file", time.Hour)
if err != nil {
	log.Fatal(err)
}
fmt.Println(req.Method, req.URL)
fmt.Println(req.Header)
```

`PresignWrite`, `PresignStat`, and `PresignDelete` work the same way for the
other operations.
