## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [x] restore
- [ ] rename
- [x] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Configuration

Use [`crate::GcsConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Listing deleted objects

`deleted(true)` includes retained soft-deleted generations alongside live
objects. Add `versions(true)` to include readable noncurrent generations as
well. In a versioned bucket, deleting by name can leave a readable noncurrent
generation. It appears in version listings until it is soft-deleted or purged.

The option uses the authenticated default Google Cloud Storage JSON endpoint
and requires a bucket with a soft-delete policy. Without that policy, the list
returns the GCS request error. Soft-deleted objects that require a hierarchical
namespace restore token return `Unsupported`. Each listed soft-deleted object
has its generation and `is_deleted` flag. Listing does not determine its
`is_current` status.

## Restore

Restore uses the authenticated Google Cloud Storage JSON API. A path-only
restore leaves a live object unchanged. If the path is absent, it selects the
most recently deleted recoverable generation by comparing when generations
became noncurrent or soft-deleted. A concurrent writer that recreates the live
object wins the destination precondition.

A version-selected restore takes a GCS generation ID and creates a new live
object from that generation. `if_not_exists` applies a destination generation
precondition to both rewrite and soft-delete restoration. Restoring a
soft-deleted generation preserves the retained source. Recovery depends on
Object Versioning or Soft Delete and on the caller's object permissions.
Soft-deleted generations that require a hierarchical namespace restore token
return `Unsupported`.

## Options to authenticate to GCS

OpenDAL supports the following authentication options:

1. Provide a base64-ed JSON key string with `credential`
2. Provide a JSON key file at explicit path with `credential_path`
3. Provide a JSON key file at implicit path
   - `GcsBackend` will attempt to load Service Account key from [ADC well-known places](https://cloud.google.com/docs/authentication/application-default-credentials).
4. Fetch access token from [VM metadata](https://cloud.google.com/docs/authentication/rest#metadata-server)
   - Only works when running inside Google Cloud.
   - If a non-default Service Account name is required, set with `service_account`. Otherwise, nothing need to be set.
5. A custom credential provider chain via `GcsBuilder.credential_provider_chain()`

Notes:

- When a Service Account key is provided, it will be used to create access tokens (VM metadata will not be used).
- Explicit Service Account key, in json or path, always take precedence over ADC-defined key paths.
- Due to [limitation in GCS](https://cloud.google.com/storage/docs/authentication/signatures#signing-process), a private key is required to create Pre-signed URL. Currently, OpenDAL only supports Service Account key.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_gcs::Gcs;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Gcs::default()
       // set the storage bucket for OpenDAL
       .bucket("test")
       // set the working directory root for GCS
       // all operations will happen within it
       .root("/path/to/dir")
       // set the credentials with service account
       .credential("service account JSON in base64")
       // set the predefined ACL for GCS
       .predefined_acl("publicRead")
       // set the default storage class for GCS
       .default_storage_class("STANDARD");

    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```
