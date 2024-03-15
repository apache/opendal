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
- [x] scan
- [x] presign
- [ ] blocking

## Configuration

- `root`: Set the work directory for backend
- `bucket`: Set the container name for backend
- `endpoint`: Customizable endpoint setting
- `credential`: Credentials string for GCS service OAuth2 authentication
- `credential_path`: Local path to credentials file for GCS service OAuth2 authentication
- `predefined_acl`: Predefined ACL for GCS
- `default_storage_class`: Default storage class for GCS

Refer to public API docs for more information.

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Gcs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Gcs::default();

    // set the storage bucket for OpenDAL
    builder.bucket("test");
    // set the working directory root for GCS
    // all operations will happen within it
    builder.root("/path/to/dir");
    // set the credentials string of service account
    builder.credential("service account credential");
    // set the predefined ACL for GCS
    builder.predefined_acl("publicRead");
    // set the default storage class for GCS
    builder.default_storage_class("STANDARD");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
