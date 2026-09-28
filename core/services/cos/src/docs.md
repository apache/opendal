## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [ ] rename
- [x] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Configuration

Use [`crate::CosConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_cos::Cos;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Cos::default()
        // set the storage bucket for OpenDAL
        .bucket("test")
        // set the endpoint for OpenDAL
        .endpoint("https://cos.ap-singapore.myqcloud.com")
        // Set the access_key_id and secret_access_key.
        //
        // OpenDAL will try load credential from the env.
        // If credential not set and no valid credential in env, OpenDAL will
        // send request without signing like anonymous user.
        .secret_id("secret_id")
        .secret_key("secret_access_key");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```
