- Proposal Name: `move_read_range_to_reader`
- Start Date: 2026-06-01
- RFC PR: [apache/opendal#7660](https://github.com/apache/opendal/pull/7660)
- Tracking Issue: [apache/opendal#7661](https://github.com/apache/opendal/issues/7661)

# Summary

Change OpenDAL's raw `Access::read` contract so it returns a reusable raw
reader for a path and read condition set. Byte ranges move from `Access::read`
arguments to methods on the returned reader.

Today `Access::read(path, OpRead { range, .. })` opens one concrete range stream.
This matches HTTP object stores, where every range read is a request, but it is a
poor fit for handle based services such as `fs`, `hdfs`, `sftp`, and
`monoiofs`. Those services can keep an opened handle and execute multiple
range reads against it, but the current raw API forces the core reader planner to
call `Access::read` again for every planned range.

This RFC keeps the `Access::read` operation, but changes what the returned
reader means:

- `Access::read(path, OpRead)` creates a reusable raw `oio::Read`.
- `oio::Read::stream(range)` creates a range-scoped byte stream.
- `oio::Read::read(range)` reads one range into a `Buffer`.
- `oio::Read::fetch(ranges)` reads multiple ranges and returns buffers in input
  order.

The public `Operator::read`, `Operator::reader`, `Reader::read`,
`Reader::fetch`, and stream conversion APIs keep their existing user-facing
shape. The change is in the raw contract and the internal execution plan.

# Motivation

OpenDAL's public `Reader` is already range based and cloneable. Users can call:

```rust
let reader = op.reader("path/to/file").await?;

let head = reader.read(0..1024).await?;
let tail = reader.read(4096..8192).await?;
let parts = reader.fetch(vec![0..1024, 4096..8192]).await?;
```

However, the current implementation plans these public calls by generating new
raw range streams. `ReadGenerator::next_reader` and `ChunkedReader` both call
`Access::read(path, args.with_range(range))` for every range. For `s3`, `gcs`,
and `azblob`, this is natural: each range is a new HTTP request. For `fs` and
`hdfs`, this means every range repeats open and seek work even though the target
data can be represented by a reusable handle.

The root cause is that `Access::read` is currently modeled as a transport-level
range request. OpenDAL needs a storage-neutral raw model:

- Object stores can implement a raw reader that issues a ranged request on
  each `stream(range)`.
- File-like services can implement a raw reader that opens once lazily and
  uses positioned reads for `stream`, `read`, and `fetch`.
- In-memory or database-backed services can implement a raw reader that
  slices cached values directly.

This keeps the public range-based reader model from RFC-4382 while removing the
S3-specific assumption from the raw API.

# Guide-level explanation

Users do not need a new API.

Single reads still work:

```rust
let data = op.read("path/to/file").await?;
let part = op.read_with("path/to/file").range(1024..2048).await?;
```

Reusable readers still work:

```rust
let reader = op.reader("path/to/file").await?;

let first = reader.read(0..1024).await?;
let second = reader.read(4096..8192).await?;
let ranges = reader.fetch(vec![0..1024, 4096..8192]).await?;
```

The difference is internal. OpenDAL creates one reusable raw reader for
`reader("path/to/file")`. After that, range operations are executed by this raw
reader instead of reopening through `Access::read` for every range.

For HTTP object stores, the raw reader stores the path and read arguments, then
sends a new ranged request for each range. Behavior and cost stay the same.

For handle based services, the raw reader may lazily open the file handle once
and reuse it for subsequent range reads. For example, `fs` can use positioned
reads against a shared file handle, and `hdfs` can use `pread`-style APIs.

# Reference-level explanation

## Raw read contract

`Access::read` remains the raw read operation:

```rust,ignore
pub trait Access: Send + Sync + Debug + Unpin + 'static {
    type Reader: oio::Read;

    fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> impl Future<Output = Result<(RpRead, Self::Reader)>> + MaybeSend;
}
```

The meaning changes:

- `path` and `OpRead` select a path and read condition set.
- The returned `Self::Reader` is a reusable raw read executor for that path and
  condition set.
- No range is selected by `Access::read`.

`OpRead` should no longer contain `BytesRange`:

```rust,ignore
pub struct OpRead {
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<Timestamp>,
    if_unmodified_since: Option<Timestamp>,
    override_content_type: Option<String>,
    override_cache_control: Option<String>,
    override_content_disposition: Option<String>,
    version: Option<String>,
}
```

`OpReader` remains an internal execution policy used by OpenDAL's public
`Reader`. It controls `concurrent`, `chunk`, `gap`, `prefetch`, and
`content_length_hint`. It must not become part of the backend `Access::read`
contract.

`ReadOptions` and `ReaderOptions` should be converted differently:

```rust,ignore
impl From<options::ReadOptions> for (OpRead, BytesRange, OpReader);
impl From<options::ReaderOptions> for (OpRead, OpReader);
```

## Reader and range stream split

The current `oio::Read` is a range-scoped byte stream. It should be renamed to a
stream concept:

```rust,ignore
pub type ReadStream = Box<dyn ReadStreamDyn>;

pub trait ReadStream: Unpin + Send + Sync {
    fn read(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend;

    fn read_all(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend {
        async {
            let mut bufs = vec![];
            loop {
                match self.read().await {
                    Ok(buf) if buf.is_empty() => break,
                    Ok(buf) => bufs.push(buf),
                    Err(err) => return Err(err),
                }
            }
            Ok(bufs.into_iter().flatten().collect())
        }
    }
}
```

The new `oio::Read` becomes the reusable raw reader:

```rust,ignore
pub type Reader = Arc<dyn ReadDyn>;

pub trait Read: Send + Sync + Debug + Unpin + 'static {
    fn stream(
        &self,
        range: BytesRange,
    ) -> impl Future<Output = Result<(RpRead, ReadStream)>> + MaybeSend;

    fn read(
        &self,
        range: BytesRange,
    ) -> impl Future<Output = Result<(RpRead, Buffer)>> + MaybeSend {
        async {
            let (rp, mut stream) = self.stream(range).await?;
            let buffer = stream.read_all().await?;
            Ok((rp, buffer))
        }
    }

    fn fetch(
        &self,
        ranges: Vec<BytesRange>,
    ) -> impl Future<Output = Result<(RpRead, Vec<Buffer>)>> + MaybeSend {
        async {
            let mut observed = RpRead::default();
            let mut out = Vec::with_capacity(ranges.len());

            for range in ranges {
                let (rp, buffer) = self.read(range).await?;
                observed.merge_if_absent(rp);
                out.push(buffer);
            }

            Ok((observed, out))
        }
    }
}
```

The exact object-safe `ReadDyn` and `ReadStreamDyn` forms should follow the
existing `oio::Read` / `ReadDyn` pattern.

## `fetch` semantics

`oio::Read::fetch` is a batch range primitive for one raw reader.

Semantics:

- `ranges` are read from the same path and read condition set.
- The returned `Vec<Buffer>` has the same length and order as the input ranges.
- `fetch(vec![])` returns an empty vector and should not perform storage I/O.
- `fetch` must not apply public `gap` merging. The public `Reader` is
  responsible for deciding which ranges should be read.
- `fetch` must not own public `concurrent` or `prefetch` policy. The public
  `Reader` is responsible for scheduling calls into the raw reader.
- `RpRead` represents read response metadata observed while serving the batch.
  It is not per-range metadata.

The public `Reader::fetch` can keep its existing behavior:

1. Merge requested ranges according to `OpReader::gap`.
2. Split the merged ranges into execution batches according to
   `OpReader::concurrent` and `OpReader::prefetch`.
3. Execute `raw_reader.fetch(batch)` for each batch.
4. Slice merged buffers back into the original requested ranges.

This preserves OpenDAL's current execution policy while allowing native readers
to optimize each batch. Backends that do not provide a specialized
implementation get the default `fetch` implementation based on repeated
`read(range)` calls.

## Public operator path

`Operator::read_options` should split public options into read args, range,
and execution policy:

```rust,ignore
async fn read_inner(acc: Accessor, path: String, opts: ReadOptions) -> Result<Buffer> {
    let (op_read, range, op_reader) = opts.into();
    let (rp, raw_reader) = acc.read(&path, op_read).await?;
    let reader = Reader::new(acc, path, raw_reader, op_reader, rp);
    reader.read(range.to_range()).await
}
```

`Operator::reader_options` should create the reusable raw reader once:

```rust,ignore
async fn reader_inner(acc: Accessor, path: String, opts: ReaderOptions) -> Result<Reader> {
    let (op_read, op_reader) = opts.into();
    let (rp, raw_reader) = acc.read(&path, op_read).await?;
    Ok(Reader::new(acc, path, raw_reader, op_reader, rp))
}
```

For services where `Access::read` currently performs remote I/O, the returned
raw reader should be lazy. This preserves the lazy reader behavior: creating
`op.reader(path)` should not require opening the underlying data request.

## Read context

`ReadContext` should hold the reusable raw reader:

```rust,ignore
pub struct ReadContext {
    acc: Accessor,
    path: String,
    args: OpRead,
    options: OpReader,
    reader: oio::Reader,
    metadata: OnceLock<Metadata>,
}
```

`acc`, `path`, and `args` are still useful for planning, especially when
OpenDAL needs `stat` to resolve unbounded ranges for chunked reads or
`AsyncSeek` adapters. Actual range I/O should go through `reader.stream`,
`reader.read`, or `reader.fetch`, not through repeated `acc.read` calls.

`Reader::metadata()` remains cache-only. The cache is updated from `RpRead`
returned by `stream`, `read`, or `fetch`.

## Layers

Existing layers continue to wrap `Access::read`, but the wrapping point moves
from a range stream to a reusable raw reader.

### Type eraser

The type eraser should erase raw readers:

```rust,ignore
async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, oio::Reader)> {
    self.inner
        .read(path, args)
        .await
        .map(|(rp, r)| (rp, Arc::new(r) as oio::Reader))
}
```

### Complete layer

`CompleteLayer` currently reads `args.range().size()` in `Access::read` and
wraps the returned range stream. After this RFC, range size is only known when
`stream`, `read`, or `fetch` is called.

`CompleteLayer` should wrap the raw reader and validate each returned range
body:

- `stream(range)` wraps the returned `ReadStream` with `CompleteReadStream`.
- `read(range)` checks the returned buffer length if `range.size()` is known.
- `fetch(ranges)` checks each buffer against the corresponding range if
  `range.size()` is known.

### Error context layer

`ErrorContextLayer` should wrap the raw reader and attach operation context
to every range operation:

- service
- path
- range
- already-read bytes for streams

### Correctness check layer

`CorrectnessCheckLayer` can keep checking `read_with_*` capabilities in
`Access::read`, because those options still belong to the path and read
condition set.

## Presign read

Presigned reads still describe one concrete request, so they still need a
range. This range should not move back into `OpRead`. Instead,
`PresignOperation::Read` should carry range separately:

```rust,ignore
pub enum PresignOperation {
    Read {
        args: OpRead,
        range: BytesRange,
    },
    // ...
}
```

`presign_read_options` converts `ReadOptions` into `(OpRead, BytesRange,
OpReader)` and discards `OpReader`.

## Backend implementation model

### HTTP object stores

Services like `s3`, `gcs`, `azblob`, `oss`, and `http` should return a reader
that stores `core`, `path`, and `OpRead`.

Each `stream(range)` sends the same request that the current `Access::read`
sends today. For example, S3's current logic:

```rust,ignore
let resp = self.core.s3_get_object(path, args.range(), &args).await?;
```

becomes reader logic:

```rust,ignore
let resp = self.core.s3_get_object(&self.path, range, &self.args).await?;
```

This keeps object-store behavior unchanged.

### Memory and database-like services

Services that already have the data content in memory can store the content or
lookup key in their raw reader. `read(range)` and `fetch(ranges)` can slice
buffers directly.

### Handle based services

`fs`, `hdfs`, `hdfs-native`, `sftp`, and `monoiofs` can reuse opened handles:

- `fs`: lazily open a file and use platform positioned reads.
- `hdfs`: lazily open a file and use `pread`-style APIs.
- `hdfs-native`: store `FileReader` and use `read_range` /
  `read_range_stream`.
- `monoiofs`: reuse its existing worker model that already performs
  `read_at`.
- `sftp`: keep the connection and file handle in the raw reader.

These readers can implement `fetch` by executing multiple positioned reads
against the same handle.

# Drawbacks

- This is a breaking raw API change. Third-party `Access` implementations must
  be updated.
- It touches many services because every `Access::read` implementation currently
  receives a range.
- The rename from range-scoped `oio::Read` to `oio::ReadStream` is large but
  necessary to avoid overloading one trait with two meanings.
- Some services may need extra lazy state to preserve the current
  `op.reader(path)` behavior.

# Rationale and alternatives

## Add `Access::open_read_at`

Rejected. It adds a parallel backend operation for an execution optimization.
OpenDAL's raw operation surface should stay centered on semantic operations:
`stat`, `read`, `write`, `list`, `delete`, `copy`, and `rename`.

## Add a public capability for `read_at`

Rejected. Users do not get a new public operation. Whether a backend reuses an
opened handle is an internal execution detail. Public `Capability` should only
describe user-visible behavior.

## Add optional `read_at` to range streams

Rejected. A range stream is already scoped to one byte range. Deriving other
ranges from it gives one stream two responsibilities and makes layer wrapping
harder. The reusable raw reader should be the place that knows how to execute
additional ranges.

## Add `ReadPlan` beside `Access::read`

Rejected. It improves execution planning but keeps the old range-scoped
`Access::read` as the primary contract. This RFC fixes the raw contract instead
of adding a second planning hook.

# Prior art

## RFC-4382 Range Based Read API

RFC-4382 moved OpenDAL's public `Reader` toward stateless range reads and called
out future `read_ranges` and native `read_at` support. This RFC keeps that
public direction but changes the raw contract so raw readers can implement
those optimizations without repeated `Access::read` calls.

## object_store

`object_store` exposes both single-range and multi-range reads through
`get_range` and `get_ranges`. OpenDAL's public `Reader::fetch` serves a similar
user-facing purpose. This RFC places the raw batch primitive on the raw
reader, where both object-store requests and file-handle positioned reads can be
implemented.

# Unresolved questions

## Fetch batching policy

The initial implementation can preserve current public behavior by batching
merged ranges according to `OpReader::concurrent`. We should benchmark whether
the default policy should prefer one large `raw.fetch` call, several batches, or
one task per merged range for different backend classes.

## Native preferred read size

This RFC keeps `OpReader` as OpenDAL's execution policy and does not introduce
backend preferred read sizes. A later RFC can revisit service-provided execution
hints if we have enough benchmark evidence.

# Future possibilities

- Native multi-range HTTP requests for services that support them.
- Platform-specific positioned read implementations for `fs`.
- Better `fetch` batching heuristics based on workload and service class.
- Integration with future completion-based I/O support.
