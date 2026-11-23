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

Use [`crate::ObsConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Credentials

OpenDAL resolves OBS credentials in this order:

1. `access_key_id`, `secret_access_key`, and `security_token` set on the builder
   or config. The signer attaches `security_token` as the `x-obs-security-token`
   header when it is set.
2. `HUAWEI_CLOUD_ACCESS_KEY_ID`, `HUAWEI_CLOUD_SECRET_ACCESS_KEY`, and
   `HUAWEI_CLOUD_SECURITY_TOKEN` loaded from the environment.

[`crate::Obs::disable_config_load`] removes the environment step so that only
configured credentials are used. [`crate::Obs::skip_signature`] skips credential
loading and request signing, which sends requests without authentication.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_obs::Obs;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Obs::default()
        // set the storage bucket for OpenDAL
        .bucket("test")
        .endpoint("obs.cn-north-1.myhuaweicloud.com")
        // Set the access_key_id and secret_access_key.
        //
        // OpenDAL will try load credential from the env.
        // If credential not set and no valid credential in env, OpenDAL will
        // send request without signing like anonymous user.
        .access_key_id("access_key_id")
        .secret_access_key("secret_access_key");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```
