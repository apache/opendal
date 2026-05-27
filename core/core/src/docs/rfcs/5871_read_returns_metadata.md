- Proposal Name: `read_returns_metadata`
- Start Date: 2025-03-24
- RFC PR: [apache/opendal#5871](https://github.com/apache/opendal/pull/5871)
- Tracking Issue: [apache/opendal#5872](https://github.com/apache/opendal/issues/5872)

# Summary

Expose the metadata returned by read operations without requiring users or adapters to issue a separate `stat` request.

Read metadata is bound to a concrete read stream. Calling `metadata().await` on the stream opens the underlying read request, stores the returned metadata and reader, and lets subsequent stream consumption reuse the same reader.

`Reader` also maintains a best-effort cache of complete object metadata that has been observed from successful read opens.

# Motivation

Today, users who need metadata while reading must issue an additional `stat()` request. This is inefficient for services that already return useful metadata from their read APIs, such as S3 `GetObject`, GCS object download, and Azure Blob download. It can also observe a different object version if the object changes between read and stat.

This is especially important for adapters such as `object_store_opendal`. `object_store::GetResult` must contain `ObjectMeta`, returned range, attributes, and a streaming payload at the time `get_opts()` returns. A stat-first implementation can satisfy that contract, but it adds an extra request before every ranged read. OpenDAL should expose read-open object metadata through its public API so these adapters can construct their result without issuing a redundant `stat`.

# Guide-level explanation

Existing high-level read APIs continue to work as before:

```rust,ignore
let data = op.reader("path/to/file").await?.read(..).await?;
```

Callers that need metadata create a stream and call `metadata().await` before consuming it:

```rust,ignore
use futures::TryStreamExt;

let mut stream = op
    .reader("path/to/file")
    .await?
    .into_bytes_stream(1024..2048)
    .await?;

let meta = stream.metadata().await?;
if let Some(etag) = meta.etag() {
    println!("ETag: {etag}");
}

let data = stream.try_collect::<Vec<_>>().await?;
```

`metadata().await` opens the read request if it has not been opened yet. The stream caches both the metadata and the underlying reader, so consuming the stream afterwards does not open a second read request.

If users do not call `metadata().await`, streams keep their existing lazy behavior: the underlying read request is opened when the stream is first polled.

`Reader` also exposes the complete object metadata it has already observed:

```rust,ignore
let reader = op.reader("path/to/file").await?;
let data = reader.read(1024..2048).await?;

if let Some(meta) = reader.metadata() {
    println!("object size: {}", meta.content_length());
}
```

`Reader::metadata()` does not perform I/O. It returns `None` until a read opened by this reader has observed enough information to build complete object metadata.

# Reference-level explanation

## Reader API

Add a cache-only metadata accessor to `Reader`:

```rust,ignore
impl Reader {
    pub fn metadata(&self) -> Option<Metadata>;
}
```

`Reader::metadata()` returns complete object metadata observed from previous successful read opens by this reader.

Semantics:

- It never performs I/O.
- It initially returns `None`.
- It returns object metadata.
- Its `content_length` is always the full object size.
- It is updated from metadata returned by successful read opens.
- If several reads are opened by the same reader, the cache stores the first object metadata observed by those reads.

This API is an ergonomic cache for users who read through `Reader::read(...)` and then want the complete object metadata observed along the way.

## Stream API

Add metadata accessors to read stream types:

```rust,ignore
impl BufferStream {
    pub async fn metadata(&mut self) -> Result<Metadata>;
}

impl FuturesBytesStream {
    pub async fn metadata(&mut self) -> Result<Metadata>;
}
```

Semantics:

- The returned metadata describes the object being read.
- Its `content_length` is always the full object size, even for ranged reads.
- The first `metadata().await` call prepares the stream by opening the underlying read request.
- The stream stores the returned `raw::RpRead::metadata` and the returned `oio::Reader`.
- Later `metadata().await` calls return the cached metadata.
- Later `poll_next` calls consume the cached reader and must not open the same read request again.
- `metadata().await` does not read the response body. It only opens the read and observes response metadata.

## Changes to `raw::RpRead`

`raw::RpRead` becomes the metadata carrier for a successful read open:

```rust,ignore
pub struct RpRead {
    metadata: Metadata,
}

impl RpRead {
    pub fn new(metadata: Metadata) -> Self;

    pub fn metadata(&self) -> &Metadata;

    pub fn into_metadata(self) -> Metadata;
}
```

`RpRead::size()` and `RpRead::range()` should be removed. Metadata should expose object metadata only; returned ranges are derived from the read request and the object size.

Callers that need the object size should use:

```rust,ignore
rp.metadata().content_length()
```

## `content_length` Semantics

`Metadata::content_length` always describes the full object size.

```text
content_length = full object size
```

For example, reading `1024..2048` from a 10 MiB object should still produce:

```text
content_length = 10485760
```

HTTP response mapping:

```text
HTTP 200 + Content-Length: N
  -> content_length = N

HTTP 206 + Content-Range: bytes start-end/total
  -> content_length = total

HTTP 206 + Content-Range: bytes start-end/*
  -> content_length cannot be derived from the response alone

HTTP 206 + Content-Length: M, no Content-Range
  -> content_length cannot be derived from the response alone
```

Backends must not expose HTTP `Content-Range` through public `Metadata`. They may parse it internally to populate the full object size.

## Implementation Details

Backends must return metadata for every successful read open.

For services that return metadata in read responses:

- Capture metadata from the read response.
- Populate `content_length` with the full object size. HTTP services may derive it from `Content-Length` for full responses or from `Content-Range` for ranged responses.
- Populate fields such as `content_type`, `etag`, `version`, `last_modified`, and user metadata when available from the response.

For services whose read primitive does not naturally return metadata:

- The backend must still satisfy the `RpRead { metadata }` contract.
- File-like services may use file metadata, seek, or an equivalent backend-local primitive during read open.
- Remote file-like services such as SFTP, HDFS, and FTP may need an additional status or size request to satisfy the contract.

This cost is part of making read metadata a mandatory read-open contract instead of an optional hint.

## Stream State

`BufferStream` should internally support a prepare/open step:

```text
metadata().await
  -> prepare/open if needed
  -> accessor.read(path, args_with_range).await
  -> cache RpRead.metadata
  -> update Reader metadata cache
  -> cache returned oio::Reader
  -> return cached metadata

poll_next()
  -> prepare/open if needed
  -> consume cached oio::Reader
```

For non-chunked streaming reads, one stream maps to one underlying `accessor.read`.

For chunked or concurrent reads, a single physical read response may only return one chunk body, but its metadata still describes the full object.

## `object_store_opendal` Integration

With stream-bound metadata, `object_store_opendal::get_opts` can avoid the stat-first path for full, bounded, and offset reads:

```rust,ignore
let reader = self.inner.reader_with(&raw_location)
    .version(...)
    .if_match(...)
    .if_none_match(...)
    .if_modified_since(...)
    .if_unmodified_since(...)
    .await?;

let mut stream = reader.into_bytes_stream(read_range.clone()).await?;
let meta = stream.metadata().await?;

let result = GetResult {
    payload: GetResultPayload::Stream(Box::pin(stream.map_err(...))),
    meta: ObjectMeta {
        location: location.clone(),
        last_modified: meta.last_modified().and_then(timestamp_to_datetime).unwrap_or_default(),
        size: meta.content_length(),
        e_tag: meta.etag().map(ToString::to_string),
        version: meta.version().map(ToString::to_string),
    },
    range: build_returned_range(read_range, meta.content_length()),
    attributes: build_attributes(&meta),
};
```

Cases that still need stat-first behavior:

- `head = true`, because the caller explicitly asks for no body.
- suffix ranges, because OpenDAL's public range input does not currently express suffix ranges.
- responses that cannot provide the full object size required by `ObjectMeta::size`.
- compatibility paths that intentionally preserve existing out-of-range behavior.

# Drawbacks

- `RpRead` becomes a stronger backend contract: successful read open must return metadata.
- Some backends will need extra work during read open to obtain size or status.
- Stream implementations become more complex because they need a reusable prepare/open state.

# Rationale and alternatives

This design keeps existing `Reader::read()` and `Operator::read()` return types unchanged. Users can opt into read-open metadata by working with read streams, or inspect the complete object metadata already observed by `Reader`.

Using `Reader::metadata()` as the only metadata API is not enough: it is not tied to a concrete read open and cannot satisfy APIs that must return metadata before handing out a streaming payload. Another alternative is returning a new `ReadResult` carrier from read APIs, but that would be a broader public API change and would duplicate the existing stream abstraction.

Removing `RpRead::size()` and `RpRead::range()` avoids a second metadata surface. Object size is represented by `metadata.content_length()`, and returned range information is derived from the read request.

# Prior art

Similar patterns exist in other storage SDKs:

- `object_store` returns metadata in `GetResult` from `get_opts`.
- AWS S3 SDK returns response metadata in `GetObjectOutput`.
- Azure Blob SDK returns properties and metadata in download responses.

# Unresolved questions

- Whether OpenDAL should add a public suffix-range input in the future so suffix reads can also use the no-stat path.
- How much logical metadata synthesis should be supported for chunked and concurrent streams.

# Future possibilities

- `object_store_opendal` can use stream metadata to avoid stat-first bounded range reads.
- `ReadContext::parse_into_range` can use read metadata or user-provided content length hints to avoid additional stat calls for open-ended ranges.
