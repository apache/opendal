- Proposal Name: `read_returns_metadata`
- Start Date: 2025-03-24
- RFC PR: [apache/opendal#5871](https://github.com/apache/opendal/pull/5871)
- Tracking Issue: [apache/opendal#5872](https://github.com/apache/opendal/issues/5872)

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

For `reader` API, we will introduce a new method `metadata()` that returns metadata:

```rust
// Before
let data = op.reader("path/to/file").await?.read(..).await?;
let meta = op.stat("path/to/file").await?;
if let Some(etag) = meta.etag() {
    println!("ETag: {}", etag);
}

// After
let reader = op.reader("path/to/file").await?;
let meta = reader.metadata();
if let Some(etag) = meta.etag() {
    println!("ETag: {}", etag);
}
let data = reader.read(..).await?;
```
The new API will be provided alongside existing functionality, allowing users to continue using current `reader` methods without modification.

For backward compatibility and to minimize migration costs, We won't change the existing `read` API. Anyone who wants 
to obtain metadata during reading can use the new reader operations instead.

# Reference-level explanation

## Changes to `Reader` API

The `impl Reader` will be modified to include a new function `metadata()` that returns metadata.

```rust
impl Reader {
    // Existing fields...
    
    fn metadata(&self) -> &Metadata {}
}
```

## Changes to struct `raw::RpRead`

The `raw::RpRead` struct will be modified to include a new field `metadata` that stores the metadata returned by the read operation.
Existing fields will be evaluated and potentially removed if they become redundant.

```rust
pub struct RpRead {
    // New field to store metadata 
    metadata: Metadata,
}
```


## Implementation Details

For services that return metadata in their read responses:
- The metadata will be captured from the service response.
- All available fields (content_type, etag, version_id, last_modified, etc.) will be populated

For services that don't return metadata in read responses:
- We'll make an additional `stat` call to fetch the metadata and populate the `metadata` field in `raw::RpRead`.

Special considerations:
- We should always return total object size in the metadata, even if it's not part of the read response
- For range reads, the metadata should reflect the full object's properties (like total size) rather than the range
- For versioned objects, the metadata should include version information if available

# Drawbacks

- Additional memory overhead for storing metadata during reads
- Potential complexity in handling metadata for range reads

# Rationale and alternatives

- Maintains full backward compatibility with existing read operations
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

- Once we return metadata during reader initialization, we can optimize `ReadContext::parse_into_range` by using the 
`content_length` from `metadata` directly, eliminating the need for an additional `stat` call
