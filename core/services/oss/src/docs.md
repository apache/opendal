# Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [ ] rename
- [x] presign

# Configuration

- `root`: Set the work dir for backend.
- `bucket`: Set the container name for backend.
- `endpoint`: Set the endpoint for backend.
- `addressing_style`: Set the addressing style for endpoint.
- `presign_endpoint`: Set the endpoint for presign.
- `presign_addressing_style`: Set the addressing style for presign endpoint.
- `access_key_id`: Set the access_key_id for backend.
- `access_key_secret`: Set the access_key_secret for backend.
- `role_arn`: Set the role of backend.
- `oidc_token`: Set the oidc_token for backend.
- `allow_anonymous`: Set the backend access OSS in anonymous way.

Refer to [`OssBuilder`]'s public API docs for more information.

# Example

## Via Builder

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal_service_oss::Oss;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create OSS backend builder.
    let mut builder = Oss::default()
        // Set the root for oss, all operations will happen under this root.
        //
        // NOTE: the root must be absolute path.
        .root("/path/to/dir")
        // Set the bucket name, this is required.
        .bucket("test")
        // Set the endpoint.
        //
        // For example:
        // - "https://oss-ap-northeast-1.aliyuncs.com"
        // - "https://oss-hangzhou.aliyuncs.com"
        .endpoint("https://oss-cn-beijing.aliyuncs.com")
        // Set the access_key_id and access_key_secret.
        //
        // OpenDAL will try load credential from the env.
        // If credential not set and no valid credential in env, OpenDAL will
        // send request without signing like anonymous user.
        .access_key_id("access_key_id")
        .access_key_secret("access_key_secret");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
