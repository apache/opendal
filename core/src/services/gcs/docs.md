## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [ ] rename
- [x] list
- [x] presign
- [ ] blocking

## Configuration

- `root`: Set the work directory for backend
- `bucket`: Set the container name for backend
- `endpoint`: Customizable endpoint setting
- `credential`: Service Account or External Account JSON, in base64
- `credential_path`: local path to Service Account or External Account JSON file
- `service_account`: name of Service Account
- `predefined_acl`: Predefined ACL for GCS
- `default_storage_class`: Default storage class for GCS

Refer to public API docs for more information. For authentication related options, read on.

## Options to authenticate to GCS

OpenDAL supports the following authentication options:

1. Provide a base64-ed JSON key string with `credential`
2. Provide a JSON key file at explicit path with `credential_path`
3. Provide a JSON key file at implicit path
    - `GcsBackend` will attempt to load Service Account key from [ADC well-known places](https://cloud.google.com/docs/authentication/application-default-credentials).
4. Fetch access token from [VM metadata](https://cloud.google.com/docs/authentication/rest#metadata-server)
    - Only works when running inside Google Cloud.
    - If a non-default Service Account name is required, set with `service_account`. Otherwise, nothing need to be set.
5. A custom `TokenLoader` via `GcsBuilder.customized_token_loader()`

Notes:

- When a Service Account key is provided, it will be used to create access tokens (VM metadata will not be used).
- Explicit Service Account key, in json or path, always take precedence over ADC-defined key paths.
- Due to [limitation in GCS](https://cloud.google.com/storage/docs/authentication/signatures#signing-process), a private key is required to create Pre-signed URL. Currently, OpenDAL only supports Service Account key.

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Gcs;
use opendal::Operator;

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

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
