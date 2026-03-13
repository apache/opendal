- Proposal Name: (`restore_api`)
- Start Date: 2025-02-04
- RFC PR: [apache/opendal#7178](https://github.com/apache/opendal/pull/7178)
- Tracking Issue: [apache/opendal#4321](https://github.com/apache/opendal/issues/4321)

# Summary

Implement restoring a deleted version or a file.

# Motivation

Cloud storage providers implement data recovery through different mechanisms:

- Example 1: **AWS S3** uses versioning exclusively. Deleted objects become "delete markers" and can be restored by copying a previous version or removing the delete marker.
- Example 2: **GCS and Azure Blob Storage**  provide both versioning AND soft delete as separate, independent features that can be enabled separately or together.

Currently, there is no standardized way in OpenDAL to restore deleted objects across these different paradigms. This creates challenges for downstream projects that need data recovery capabilities. For example, iceberg-java [already supports file restore](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/io/SupportsRecoveryOperations.java) in some cloud environments, while iceberg-rust, written with OpenDAL, does not provide this functionality yet.

In order to secure data pipelines, users currently have to maintain implementations of restore logic across multiple languages and storage backends. Having this functionality in OpenDAL would centralize this capability and make it available to all downstream users.

# Guide-level explanation

OpenDAL provides two ways to restore deleted files, depending on the storage backend's capabilities:

1. **Version-based restoration**: Extend the `copy` operation with an optional source `version` parameter to enable promoting non-current versions to the current version (for storage systems with versioning support)
2. **Soft delete restoration**: Add an `undelete` operation for storage systems that implement soft delete as a distinct feature from versioning (GCS and Azure Blob Storage)

We might also provide a high-level `restore` API that automatically chooses the right approach based on service capabilities, defaulting to versioning approach:

```rust
impl Operator {
    pub async fn restore(&self, path: &str, version: Option<&str>) -> Result<()> {
        let cap = self.info().full_capability();

        if let Some(v) = version {
            // Restore specific version via copy
            if cap.versioning {
                return self.copy_with(&path, &path).source_version(&v).await;
            }
        } else {
            // Restore soft-deleted via undelete
            if cap.undelete {
                return self.undelete(path).await;
            }

            // Fall back to latest version via copy
            if cap.versioning && cap.list_with_versions {
                let version = self.get_latest_version(path).await?;
                return self.copy_with(&path, &path).source_version(&version).await;
            }
        }

        Err(Error::new(ErrorKind::Unsupported, "restoration not supported"))
    }
}
```


## Approach 1: Version-Based Restoration (Copy with Version)

For storage systems with versioning enabled, you can restore a deleted file by copying a previous version:

```rust
use opendal::{Operator, Result};

async fn restore_via_version(op: Operator) -> Result<()> {
    // List versions to find the one to restore
    // let mut lister = op.lister_with("path/to/file.txt")
    //     .versions(true)
    //     .await?;

    // Pick the version you want to restore
    let version_id = "version-123";

    // Copy the old version to create a new current version
    op.copy_with("path/to/file.txt", "path/to/file.txt")
        .source_version(version_id)
        .await?;

    // The file is now restored with content from the specified version
    Ok(())
}
```

## Approach 2: Soft Delete Restoration (Undelete)

For storage systems with soft delete enabled, you can restore a soft-deleted file by using a new API endpoint `undelete`:

```rust
use opendal::{Operator, Result};

async fn restore_soft_deleted(op: Operator) -> Result<()> {
    // Check if the storage backend supports undelete
    if op.info().full_capability().undelete {
        // Restore a soft-deleted file
        op.undelete("path/to/file.txt").await?;

        // The file is now restored to its last alive state
        let content = op.read("path/to/file.txt").await?;
    }
    Ok(())
}
```

## Choosing the Right Approach

The choice depends on your storage configuration:

```rust
let cap = operator.info().full_capability();

if cap.undelete {
    // Use undelete for soft-deleted files
    operator.undelete("deleted_file.txt").await?;
} else if cap.versioning {
    // Use copy with version for versioned storage
    let version_id = /* get version from listing */;
    operator.copy_with("deleted_file.txt", "deleted_file.txt")
        .source_version(version_id)
        .await?;
} else {
    // Backend doesn't support restoration
    return Err(Error::new(ErrorKind::Unsupported, "restoration not supported"));
}
```

## Error Handling

### Copy with Version Errors

```rust
match op.copy_with("file.txt", "file.txt")
    .source_version(version_id)
    .await
{
    Ok(_) => println!("Version restored successfully"),
    Err(e) if e.kind() == ErrorKind::NotFound => {
        // Source version doesn't exist in history
        println!("Version {} not found", version_id);
    }
    Err(e) if e.kind() == ErrorKind::Unsupported => {
        println!("Copy with version not supported by this backend");
    }
    Err(e) => println!("Copy failed: {}", e),
}
```

When copying to a non-existing destination path, the behavior follows the existing `copy` operation semantics (typically succeeds, creating the destination).

### Undelete Errors

```rust
match op.undelete("file.txt").await {
    Ok(_) => {
        // Success: file was restored from soft-deleted state
        // OR file was already alive (idempotent success)
        println!("File is now alive");
    }
    Err(e) if e.kind() == ErrorKind::NotFound => {
        // No soft-deleted object with this path exists
        println!("No soft-deleted file found at this path");
    }
    Err(e) if e.kind() == ErrorKind::Unsupported => {
        println!("Undelete not supported by this backend");
    }
    Err(e) => println!("Undelete failed: {}", e),
}
```

**Important**: `undelete` is idempotent. If the file already exists and is alive, calling `undelete` succeeds (returns `Ok`). This allows for safe retry logic and simplifies recovery workflows.

# Reference-level explanation

## Approach 1: Copy with Source Version

### Extended Copy API

The `OpCopy` operation is extended with an optional source version parameter:

```rust
#[derive(Debug, Clone, Default)]
pub struct OpCopy {
    // ... existing fields
    source_version: Option<String>,
}

impl OpCopy {
    pub fn with_source_version(mut self, version: &str) -> Self {
        self.source_version = Some(version.to_string());
        self
    }

    pub fn source_version(&self) -> Option<&str> {
        self.source_version.as_deref()
    }
}
```

### Capability Flag

No new capability flag is needed for copy with version. The existing `versioning` capability flag indicates whether the backend supports versioned operations, including copying from specific versions:

```rust
pub struct Capability {
    // ... other fields
    pub versioning: bool, // existing capability
}
```

### Public API Extension

```rust
impl Operator {
    pub fn copy_with(&self, from: &str) -> FutureCopy {
        FutureCopy::new(self.clone(), from.to_string())
    }
}

pub struct FutureCopy {
    op: Operator,
    from: String,
    source_version: Option<String>,
}

impl FutureCopy {
    pub fn source_version(mut self, version: impl Into<String>) -> Self {
        self.source_version = Some(version.into());
        self
    }

    pub async fn run(self, to: impl AsRef<str>) -> Result<()> {
        let from = normalize_path(&self.from);
        let to = normalize_path(to.as_ref());

        let mut args = OpCopy::new(&from, &to);
        if let Some(version) = self.source_version {
            args = args.with_source_version(&version);
        }

        self.op.inner().copy(&from, &to, args).await?;
        Ok(())
    }
}
```

### Operation Contract

The extended `copy` operation with source version has the following contract:

- **Purpose**: Copy content from a specific version of an object to a destination path
- **Success condition**: Returns `Ok(RpCopy)` when the version is successfully copied to the destination
- **Error conditions**:
  - Returns `ErrorKind::NotFound` if the source version doesn't exist in the object's history
  - Returns `ErrorKind::Unsupported` if the backend doesn't support versioned copy
  - Other errors follow the existing `copy` operation semantics
- **Capability requirement**: The `Capability::versioning` flag must be `true` for versioned copy to be available
- **Destination behavior**: Follows existing `copy` semantics (typically creates destination if it doesn't exist)

### Implementation Pattern

For backends with versioning support (S3, GCS, Azure), the `copy` implementation checks for the source version:

```rust
async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
    let source_path = if let Some(version) = args.source_version() {
        // Include version in the source path/query parameters
        format!("{}?versionId={}", from, version)
    } else {
        from.to_string()
    };

    // Perform copy operation with potentially versioned source
    // ...
}
```

## Approach 2: Undelete for Soft Delete

### Operation Contract

The `undelete` operation is defined in the `Access` trait with the following contract:

- **Purpose**: Restores a soft-deleted object to its active state
- **Parameters**: Only requires the path - no version/generation parameters needed
- **Success condition**: Returns `Ok(RpUndelete)` when:
  - The path was in soft-deleted state and is now restored, OR
  - The path already exists as an alive object (idempotent success)
- **Error conditions**:
  - Returns `ErrorKind::NotFound` if no object (alive or soft-deleted) exists at the path
  - Returns `ErrorKind::Unsupported` if the backend doesn't support undelete
- **Capability requirement**: The `Capability::undelete` flag must be `true` for the operation to be available
- **Idempotency**: Calling `undelete` on an already-alive object succeeds without error
- **Backend behavior**: Implementations automatically restore the latest soft-deleted version (e.g., GCS internally discovers the generation number)

### Core Types

**Operation struct** (`OpUndelete`):
```rust
#[derive(Debug, Clone, Default)]
pub struct OpUndelete {}
```

**Response struct** (`RpUndelete`):
```rust
#[derive(Debug, Clone, Default)]
pub struct RpUndelete {}
```

**Capability flag**:
```rust
pub struct Capability {
    // ... other fields
    pub undelete: bool,
}
```

### Trait Definition

The `Access` trait is extended with:

```rust
fn undelete(
    &self,
    path: &str,
    args: OpUndelete,
) -> impl Future<Output = Result<RpUndelete>> + MaybeSend {
    let (_, _) = (path, args);
    ready(Err(Error::new(
        ErrorKind::Unsupported,
        "operation is not supported",
    )))
}
```

### Public API

Users access the functionality through:

```rust
impl Operator {
    pub async fn undelete(&self, path: &str) -> Result<()> {
        let path = normalize_path(path);
        self.inner().undelete(&path, OpUndelete::new()).await?;
        Ok(())
    }
}
```

## Layer Integration

Both operations integrate with OpenDAL's layer system. The following layers provide explicit implementations:

1. **RetryLayer**: Wraps calls with retry logic for temporary failures
2. **TimeoutLayer**: Adds timeout protection
3. **LoggingLayer**: Logs operation start, finish, and errors
4. **ConcurrentLimitLayer**: Applies semaphore-based concurrency limits

Other layers rely on default forwarding through `LayeredAccess`, which delegates to the inner layer.

## Implementation Details

### AWS S3

AWS S3 provides [soft delete through versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/versioning-workflows.html) and delete markers. Recovery is achieved by copying a previous version to the same path.

S3 does not implement `undelete` as it has no separate soft delete feature.

### Google Cloud Storage

GCS provides both [soft delete with retention policies](https://cloud.google.com/storage/docs/soft-delete) and [object versioning](https://cloud.google.com/storage/docs/object-versioning).

**For soft delete**, implement `undelete` using the [GCS restore API](https://cloud.google.com/storage/docs/use-soft-deleted-objects#restore):

```rust
pub async fn gcs_undelete_object(&self, path: &str) -> Result<Response<Buffer>> {
    // Step 1: List soft-deleted objects to find the generation number
    let list_url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o?prefix={}&softDeleted=true",
        self.bucket,
        percent_encode_path(path)
    );

    let list_req = Request::get(&list_url)
        .header(AUTHORIZATION, format!("Bearer {}", self.token()?))
        .body(Buffer::new())
        .map_err(new_request_build_error)?;

    let list_resp = self.send(list_req).await?;

    // Parse response to extract the latest generation number
    // (In practice, parse JSON and find the matching object's generation)
    let generation = parse_latest_generation(list_resp, path)?;

    // Step 2: Restore using the generation number
    let restore_url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}/restore?generation={}",
        self.bucket,
        percent_encode_path(path),
        generation
    );

    let restore_req = Request::post(&restore_url)
        .header(AUTHORIZATION, format!("Bearer {}", self.token()?))
        .header(CONTENT_TYPE, "application/json")
        .extension(Operation::Undelete)
        .body(Buffer::new())
        .map_err(new_request_build_error)?;

    self.send(restore_req).await
}
```

This implementation:
1. Lists soft-deleted objects at the path prefix
2. Extracts the latest generation number from the response
3. Calls the restore API with that generation
4. Keeps the public API simple (no generation parameter needed)

Capability:
```rust
undelete: self.config.enable_soft_deletes,
versioning: self.config.enable_versioning, // existing capability
```

**For versioning**, extend the existing `copy` implementation to support source version parameter.

### Azure Blob Storage

Azure provides both [soft delete with retention policies](https://learn.microsoft.com/en-us/rest/api/storageservices/soft-delete-for-blob-storage) and [object versioning](https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview).

**For soft delete**, use the [Undelete Blob REST API](https://learn.microsoft.com/en-us/rest/api/storageservices/undelete-blob):

```rust
pub async fn azblob_undelete_blob(&self, path: &str) -> Result<Response<Buffer>> {
    let url = format!("{}?comp=undelete", self.build_path_url(path));

    let mut req = Request::put(&url)
        .header(CONTENT_LENGTH, 0)
        .extension(Operation::Undelete)
        .body(Buffer::new())
        .map_err(new_request_build_error)?;

    self.sign(&mut req).await?;
    self.send(req).await
}
```

**For versioning**, extend the existing `copy` implementation:

```rust
pub async fn azblob_copy_blob(&self, from: &str, to: &str, version: Option<&str>) -> Result<Response<Buffer>> {
    let source_url = if let Some(v) = version {
        format!("{}?versionid={}", self.build_path_url(from), v)
    } else {
        self.build_path_url(from)
    };

    let url = self.build_path_url(to);

    let mut req = Request::put(&url)
        .header("x-ms-copy-source", source_url)
        .extension(Operation::Copy)
        .body(Buffer::new())
        .map_err(new_request_build_error)?;

    self.sign(&mut req).await?;
    self.send(req).await
}
```

Capabilities:
```rust
undelete: self.config.enable_soft_deletes,
versioning: self.config.enable_versioning, // existing capability
```

## Corner Cases

### When to Use Which Approach

| Storage Backend | Soft Delete Available | Versioning Available | Recommended Approach |
|-----------------|----------------------|----------------------|----------------------|
| AWS S3 | No (uses versioning) | Yes | Copy with version |
| GCS | Yes | Yes | Either (based on configuration) |
| Azure Blob | Yes | Yes | Either (based on configuration) |

### Soft Delete vs Versioning in GCS/Azure

In GCS and Azure Blob Storage, soft delete and versioning are independent:

- **Soft delete**: When enabled, deleted objects are marked as soft-deleted and retained for a configured period. They can be restored using `undelete` which brings back the last alive state.
- **Versioning**: When enabled, every modification creates a new version. Deleted objects create a delete marker or a special version. Restoration requires identifying a specific version and copying it.

You can enable both simultaneously in GCS. In this case:
- Use `undelete` for simple restoration of recently deleted files (simpler, no need to find version ID)
- Use `copy` with version for restoring specific historical versions or when soft delete retention has expired

Enabling both features in Azblob in some configurations is impossible.

### Backend-Specific Undelete Implementation

While the public API for `undelete` is consistent across backends (just provide the path), the internal implementation differs:

**Azure Blob Storage**:
- Single API call: `PUT {blob-url}?comp=undelete`
- Automatically restores the most recent soft-deleted version
- Simple and fast

**Google Cloud Storage**:
- Two API calls internally:
  1. `GET /b/{bucket}/o?prefix={path}&softDeleted=true` - List soft-deleted objects
  2. `POST /b/{bucket}/o/{path}/restore?generation={generation}` - Restore with generation number
- The implementation automatically discovers the latest generation before restoring
- Additional latency due to the extra list call, but maintains a simple public API

Both backends present the same simple interface to users: `op.undelete(path)`, with the complexity hidden inside the implementation.

### Idempotent Undelete

Calling `undelete` on a file that already exists and is alive **succeeds** without error. This idempotent behavior:
- Simplifies recovery workflows (no need to check if file is alive first)
- Enables safe retry logic
- Makes the operation predictable: "ensure this path is alive" rather than "restore from deleted state"

Example: Azure Blob Storage's Undelete Blob API exhibits this behavior - calling undelete on an already-active blob succeeds.

### No Object at Path

Calling `undelete` on a path where no object exists (neither alive nor soft-deleted) returns `NotFound` error.

### Expired Retention Period

If the soft delete retention period has expired, the blob is permanently deleted and `undelete` will return `NotFound`. In this case, if versioning is also enabled, you may still be able to restore using `copy` with version.

### Copy with Non-existent Version

When using `copy` with source version, if the specified version doesn't exist in the object's history, the operation returns `NotFound`.

### Copy to Non-existent Path

When using `copy` with version to a destination path, the behavior follows the existing `copy` operation semantics (typically succeeds, creating the destination object).

# Drawbacks

- **Two different APIs**: Users need to understand when to use `copy` with version vs `undelete`
- **Backend-specific behavior**: Not all storage backends support both approaches
- **Testing complexity**: Requires both soft delete and versioning configuration in storage backends for comprehensive integration testing

# Rationale and alternatives

## Why This Design

This design was chosen because:

1. **Aligns with cloud provider architecture**: Respects the distinction between soft delete and versioning that GCS and Azure make explicit
2. **Reuses existing API where appropriate**: Extending `copy` for version-based restoration is intuitive and consistent with the "copy from source" mental model
3. **Clear separation of concerns**: `undelete` is specifically for soft delete scenarios, while `copy` with version is for version-based scenarios
4. **Backend flexibility**: Each backend can implement the approach(es) that match its native capabilities
5. **User choice**: For backends supporting both (GCS/Azure), users can choose the simpler approach (undelete) or the more granular approach (copy with version)
6. **Simple public API**: The `undelete` operation requires only a path parameter across all backends. Backend-specific details (like GCS generation discovery) are handled internally, keeping the API consistent and easy to use

## Alternative 1: Undelete with Optional Version Parameter

**Description**: Have a single `undelete` API that accepts an optional version parameter.

**Pros**:
- Single API for all restoration scenarios
- Simpler mental model

**Cons**:
- **Copying a version to another path will remain unimplemented**: if one wants to create a new alive object "fileA-version123-backup" from another object's ("fileA") version, it would require restoring this version as "fileA" and then copying it into "fileA-version123-backup" destination; this would become even more complicated if "fileA" is alive all this time.
- **Less discoverable**: Users familiar with copy operations might not think to look at undelete for version restoration

## Alternative 2: Promote API

**Description**: Add a new `promote(path, version)` API for version-based restoration.

**Pros**:
- Explicit naming for version promotion
- Clear separation from undelete

**Cons**:
- **New operation**: Adds a third operation when `copy` already exists and conceptually fits
- **Limited use case**: Only works for versioning, not soft delete
- **Less intuitive**: "Promote" is less clear than "copy from version"

## Alternative 3: Undelete Only

**Description**: Only implement `undelete` and try to make it work for all backends.

**Cons**:
- **Doesn't match S3's model**: S3 has no soft delete concept, forcing undelete to be implemented via versioning feels artificial
- **Less explicit**: Users don't know if they're restoring from soft delete or versioning
- **Harder to implement**: Backends would need to check multiple features internally and choose restoration strategy

## Impact of Not Implementing

Without this feature:
- Downstream projects cannot provide data recovery features
- Data recovery workflows remain fragmented across different OpenDAL users
- The versioning feature remains incomplete without a clear restoration path

# Prior art

## AWS S3

AWS S3 provides [soft delete through versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html) and delete markers. Recovery is achieved by:
- Listing object versions
- Copying a previous version to create a new current version
- OR removing all delete markers stacked on top of the latest version (no plans to use this approach in OpenDAL)

## Azure Blob Storage

Azure provides:
- Explicit [Undelete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/undelete-blob) REST API for soft delete
- [Blob versioning](https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview) as a separate feature
- [Copy Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob) operation that supports copying from specific versions

## Google Cloud Storage

GCS provides:
- [Soft delete with retention policies](https://cloud.google.com/storage/docs/soft-delete), allowing recovery within the retention window
- [Object versioning](https://cloud.google.com/storage/docs/object-versioning) as a separate feature
- APIs to list soft-deleted objects and restore them

# Unresolved questions

- Should `copy` with source version support copying from soft-deleted versions in GCS/Azure? (e.g., combining both features)
- Should we add a helper method that automatically chooses between undelete and copy-with-version based on available capabilities?

# Future possibilities

## Batch Operations

Add batch operations for both approaches. Example:

```rust
// Batch undelete
op.undelete_iter(vec!["file1.txt", "file2.txt"]).await?;
```

## List Deleted Files

Extend capability to list soft-deleted files to all backends that support soft deletion:

```rust
let mut lister = op.lister_with("path/")
    .deleted(true)  // Include soft-deleted files
    .await?;
```
