- Proposal Name: `buffered_reader`
- Start Date: 2023-12-10
- RFC PR: [apache/incubator-opendal#3574](https://github.com/apache/incubator-opendal/pull/3734)
- Tracking Issue: [apache/incubator-opendal#3575](https://github.com/apache/incubator-opendal/issues/3735)

# Summary

Amortize overhead of IO.

# Motivation

The aim is to amortize the overhead of IO. Some scenarios, such as importing data to the database, typically require supporting multiple file formats, and the size of files varies(very large or very small). Most readers of these formats may behave similarly to the `ParquetStream`; they read and decode data piece after piece.

```
File
┌───┬───┬───┬──┐
│   │   │   │  │
└───┴───┴───┘▲─┘
             │
             │ 1. SeekFromEnd(8)

               2. ReadToEnd(limit)
```

However, OpenDAL will yield an IO request after `seek()` and `read()` are invoked. For storage services such as S3, The [research](https://www.vldb.org/pvldb/vol16/p2769-durner.pdf) shows that the 8MiB~16MiB is the preferred IO size, If the IO size is too small, the TTFB dominates the overall runtime; too large will reach the bandwidth limit.

Therefore, This RFC proposes a buffered reader to amortize the overhead of IO.

# Guide-level explanation

For users who want to buffered reader, they will call the new API `buffer`. And the default behavior remains unchanged, so users using `op.reader_with()` are not affected. The `buffer` function will take a number as input, and this number will represent the maximum buffer capability the reader is able to use. 

```rust
op.reader_with(path).buffer(32 * 1024 * 1024).await
```

If a user prefers to customize the `Preferred IO Size`, it provides a `read_size` option (default is 8MiB):

```rust
op.reader_with(path).buffer(32 * 1024 * 1024).read_size(8 * 1024 * 1024).await
```

# Reference-level explanation

There is an invariant: the `Buffer Capability` is at least greater than or equal to the `Preferred IO Size`.

**Buffering**

The target file will be split into multiple segments to align with the `Preferred IO Size`, and the buffered segments also align with the `Preferred IO Size` for efficient lookup.

```
Preferred IO Size
┌───┐
│   │
└───┘
Buffer
┌──┬────┐
│**│    │
└──┴────┘
File
┌───┬───┬───┬──┐
│   │   │   │**│
└───┴───┴───┘▲─┘
             │
             │ 3. Fetches and buffers 1 segment
```

**Buffer partial bypass**

If a read request crosses multiple segments, it fetches multiple segments in a single request. If the total size of these segments is greater than the `Buffer Capability`, it prefers to buffer the last partial read segments for future use.

```
Buffer
┌──┬────┐
│**│    │
└──┴────┘
File
┌───┬───┬───┬──┐
│   │   │   │  │
└──▲└───┴─▲─┴──┘
   │      │
   │      │ 2. ReadToEnd(limit), The limit > Cap(Buffer).
   │
   │ 1. SeekFromStart(128)
```

```
Buffer
┌──┬───┬┐
│**│///││
└──┴───┴┘
File
┌───┬───┬───┬──┐
│xxx│xxx│///│  │
└───┴───┴──▲┴──┘
           │
           │ 3. Fetches and buffers the last partial read segment
```

# Drawbacks
None

# Rationale and alternatives
None

# Prior art
None

# Unresolved questions
None

# Future possibilities
- Concurrent fetching segments
