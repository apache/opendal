---
title: Common tasks
sidebar_label: Common tasks
description: Task-oriented recipes for OpenDAL in Java — read, write, stream, list, delete, copy, and presign.
---

# Common tasks

Recipes for the things you actually do with storage. Each works on every
service. They assume an `Operator op` built as in
[Getting started](./02-getting-started.md). For full signatures, see the
[API reference](https://opendal.apache.org/docs/java/).

A few conventions used throughout:

- Paths are relative to the operator's root; a trailing `/` means a directory
  (`logs/app/` is a directory, `logs/app` is a file).
- `read` returns `byte[]`; `write` takes a `String` or `byte[]`.
- The synchronous `Operator` is shown here. `AsyncOperator` mirrors these verbs,
  returning a `CompletableFuture` from each call.

## Read a whole file

```java
byte[] data = op.read("path/to/file");
String text = new String(data);
```

## Read part of a file

`read(path, offset, length)` reads a range:

```java
byte[] data = op.read("path/to/file", 0, 1024);
```

## Stream a large file

Don't load gigabytes into memory — read through an `InputStream` in chunks:

```java
import java.io.InputStream;

try (InputStream in = op.createInputStream("big.bin")) {
    byte[] buffer = new byte[8192];
    int n;
    while ((n = in.read(buffer)) != -1) {
        // process buffer[0..n]
    }
}
```

## Write a whole file

```java
op.write("path/to/file", "Hello, World!");
op.write("path/to/file", new byte[] {0, 1, 2, 3});
```

## Stream a large upload

Write incrementally through an `OutputStream`; closing it commits the upload:

```java
import java.io.OutputStream;

try (OutputStream out = op.createOutputStream("big.bin")) {
    out.write(firstChunk);
    out.write(secondChunk);
}
```

## Check metadata

```java
import org.apache.opendal.Metadata;

Metadata meta = op.stat("path/to/file");
System.out.println(meta.getContentLength() + " bytes, dir = " + meta.isDir());
```

## List a directory

`list` returns the entries under a path:

```java
import org.apache.opendal.Entry;

for (Entry entry : op.list("dir/")) {
    System.out.println(entry.getPath() + " " + entry.getMetadata().getMode());
}
```

## Walk a tree recursively

Pass a `ListOptions` with `recursive(true)`:

```java
import org.apache.opendal.Entry;
import org.apache.opendal.ListOptions;

ListOptions options = ListOptions.builder().recursive(true).build();
for (Entry entry : op.list("dir/", options)) {
    System.out.println(entry.getPath());
}
```

## Delete a file or a whole tree

```java
op.delete("path/to/file");   // single path; succeeds even if missing
op.removeAll("dir/");        // a path and everything under it
```

## Create a directory

```java
op.createDir("path/to/dir/");   // the trailing slash is required
```

## Copy and rename

```java
op.copy("from.txt", "to.txt");
op.rename("old.txt", "new.txt");
```

Both require a service that supports them; see
[capability checks](./05-production.md#capability-checks).

## Generate a presigned URL

Hand a time-limited URL to a third party so they can access an object without
your credentials. Presigning is part of the **async** API:

```java
import java.time.Duration;
import org.apache.opendal.AsyncOperator;
import org.apache.opendal.PresignedRequest;

try (AsyncOperator op = AsyncOperator.of("s3", conf)) {
    PresignedRequest req =
        op.presignRead("path/to/file", Duration.ofHours(1)).join();
    System.out.println(req.getMethod() + " " + req.getUri());
    System.out.println(req.getHeaders());
}
```

`presignWrite` and `presignStat` produce presigned requests for the matching
operations.
</content>
