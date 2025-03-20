- Proposal Name: `read_returns_metadata`
- Start Date: 2025-03-24
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

# Summary

Enhance read operations by returning metadata along with data in read operations.

# Motivation

Currently, read operations (`read`, `read_with`, `reader`, `reader_with`) only return the data content. Users who need metadata 
during reads (like `Content-Type`, `ETag`, `version_id`, etc.) must make an additional `stat()` call. This is inefficient and 
can lead to race conditions if the file is modified between the read and stat operations.

Many storage services (like S3, GCS, Azure Blob) return metadata in their read responses. For example, S3's GetObject API returns 
important metadata like `ContentType`, `ETag`, `VersionId`, `LastModified`, etc. We should expose this information to users 
directly during read operations.

# Guide-level explanation

The read operations will be enhanced to return both data and metadata:

```rust
// Before
let data = op.read("path/to/file").await?;
let meta = op.stat("path/to/file").await?;
if let Some(content_type) = meta.content_type() {
    println!("Content-Type: {}", content_type);
}

// After
let (data, meta) = op.read("path/to/file").await?;
if let Some(content_type) = meta.content_type() {
    println!("Content-Type: {}", content_type);
}
```

For reader operations:

```rust
// Before
let data = op.reader("path/to/file").await?.read(..).await?;
let meta = op.stat("path/to/file").await?;
if let Some(etag) = meta.etag() {
    println!("ETag: {}", etag);
}

// After
let reader = op.reader("path/to/file").await?;
let (data, meta) = reader.read(..).await?;
if let Some(etag) = meta.etag() {
    println!("ETag: {}", etag);
}
```

The behavior remains backward compatible if users don't need the metadata - they can simply ignore the metadata part of the return tuple.

# Reference-level explanation

## Changes to `Operator` API

The following functions will be modified to return `Result<(Buffer, Metadata)>` instead of `Result<Buffer>`:

- `read()`
- `read_with()`

## Changes to `Reader` API

- `read()` will be modified to return `Result<(Buffer, Metadata)>` instead of `Result<Buffer>`.
- `fetch()` will be modified to return `Result<(Vec<Buffer>, Metadata)>` instead of `Result<Buffer>`.

## Changes to trait `oio::Read`

The `Read` trait will be modified to include a new function `metadata()` that returns metadata.

```rust
pub trait Read {
    // Existing functions...
    
    fn metadata(&self) -> Metadata;
}
```

## Changes to struct `http_util::HttpBody`

The `HttpBody` struct will be modified to include a new field for metadata.



## Implementation Details

For services that return metadata in their read responses:
- The metadata will be captured from the service response.
- All available fields (content_type, etag, version_id, last_modified, etc.) will be populated

For services that don't return metadata in read responses:
- for `fs`: we can use `stat` to retrieve the metadata before returning. Since the metadata is cached by the kernel, this should be efficient
- for other services: A default metadata object will be returned

Special considerations:
- We should always return total object size in the metadata, even if it's not part of the read response
- For range reads, the metadata should reflect the full object's properties (like total size) rather than the range
- For versioned objects, the metadata should include version information if available

# Drawbacks

- Minor breaking change for users who explicitly type the return value of read operations
- Additional memory overhead for storing metadata during reads
- Potential complexity in handling metadata for range reads

# Rationale and alternatives

- Provides a clean, consistent API that matches `write_returns_metadata`
- Improves performance by avoiding additional stat calls
- Aligns with common storage service APIs (S3, GCS, Azure)

# Prior art

Similar patterns exist in other storage SDKs:

- `object_store` crate returns metadata in `GetResult` after calling `get_opts`
- AWS S3 SDK returns comprehensive metadata in `GetObjectOutput`
- Azure Blob SDK returns properties and metadata in `DownloadResponse`

# Unresolved questions

None

# Future possibilities

None
