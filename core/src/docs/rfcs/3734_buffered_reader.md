- Proposal Name: `buffered_reader`
- Start Date: 2023-12-10
- RFC PR: [apache/incubator-opendal#3574](https://github.com/apache/incubator-opendal/pull/3734)
- Tracking Issue: [apache/incubator-opendal#3575](https://github.com/apache/incubator-opendal/issues/3735)

# Summary

Allowing the underlying reader to fetch data at the buffer's size to amortize the IO's overhead.

# Motivation

The objective is to mitigate the IO overhead. In certain scenarios, the reader processes the data incrementally, meaning that it utilizes the `seek()` function to navigate to a specific position within the file. Subsequently, it invokes the `read()` to reads `limit` bytes into memory and performs the decoding process.

```
File
┌───┬───┬───┬──┐
│   │   │   │  │
└───┴───┴───┘▲─┘
             │
             │ 1. SeekFromEnd(8)

               2. ReadToEnd(limit)
```

OpenDAL triggers an IO request upon invoking `read()` if the `seek()` has reset the inner state. For storage services like S3, [research](https://www.vldb.org/pvldb/vol16/p2769-durner.pdf) suggests that an optimal IO size falls within the range of 8MiB to 16MiB. If the IO size is too small, the Time To First Byte (TTFB) dominates the overall runtime, resulting in inefficiency.

Therefore, this RFC proposes the implementation of a buffered reader to amortize the overhead of IO.

# Guide-level explanation

For users who want to buffered reader, they will call the new API `buffer`. And the default behavior remains unchanged, so users using `op.reader_with()` are not affected. The `buffer` function will take a number as input, and this number will represent the maximum buffer capability the reader is able to use. 

```rust
op.reader_with(path).buffer(32 * 1024 * 1024).await
```

# Reference-level explanation


**Buffering**

When the `Buffer Capability` is specified, the underlying reader fetches and buffers data in chunks corresponding to the buffer size.

```
Buffer
┌───────┐
│       │
└───────┘
File
┌───┬───┬───┬──┐
│   │   │   │  │
└───▲───┴───▲──┘
    │       │
    │       │ 2. ReadToEnd(limit), The limit <= Cap(Buffer).
    │
    │ 1. SeekFromStart(128)

```

```
Buffer
┌───────┐
│*******│
└───────┘
File
┌───┬───────┬──┐
│   │*******│  │
└───▲───────▲──┘
     3. Fetches and buffers the data.
```

**Overlapping Read**

If a read request attempts to read data that is partially buffered, it copies the partially buffered data to the `dst` buffer first.

```
Buffer
┌───────┐
│*******│
└───────┘
File
┌───────┬──────┐
│*******│      │
└────▲──┴──▲───┘
     │     │
     │     │ 2. ReadToEnd(limit), The limit <= Cap(Buffer).
     │
     │ 1. SeekFromStart(128)
```

Then, fetches and buffers data in chunks corresponding to the buffer size.

```
Buffer
┌───────┐
│///////│
└───────┘
File
┌────┬───────┬─┐
│    │///////│ │
└────▲─────▲─┴─┘
     3. Fetches and buffers the data.
```

**Bypass**

If a read request attempts to read data larger than the `Buffer Capability`, it fetches the data bypassing the `Buffer` entirely.

```
Buffer
┌───────┐
│       │
└───────┘
File
┌───────────┬──┐
│           │**│
└──▲──────▲─┴──┘
   │      │
   │      │ 2. ReadToEnd(limit), The limit > Cap(Buffer).
   │
   │ 1. SeekFromStart(128)
```

```
Buffer
┌───────┐
│       │
└───────┘
File
┌───────────┬──┐
│           │**│
└──▲───────▲┴──┘
   3. Fetches and bypasses
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
- Concurrent fetching.
- Tailing buffering.
