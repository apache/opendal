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

Use [`crate::OssConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

### Credential resolution

OSS tries explicit access keys, environment credentials, the OSS profile file
(`~/.oss/credentials`), the Alibaba Cloud shared credentials and CLI config
files, credentials from `ALIBABA_CLOUD_CREDENTIALS_URI`, OIDC assume-role
credentials, and ECS instance RAM role credentials, in that order. The first
provider that returns credentials is used. In ACK RRSA deployments, OIDC
credentials therefore take precedence over the node's ECS metadata credentials.
If a provider returns no credentials or fails, the chain tries the next provider.
An OSS permission error does not trigger a switch to another credential provider.

When `role_arn` is set in the service configuration, these providers supply the
base credentials for the additional AK-based AssumeRole request.

Set [`OssBuilder::disable_config_load`] to stop reading ambient configuration:
the environment, `~/.oss/credentials`, the Alibaba Cloud shared credentials and
CLI config files, and `ALIBABA_CLOUD_CREDENTIALS_URI`. Explicit access keys, OIDC
options that are all set on the builder, and the `role_arn` flow keep working.
ECS instance RAM role credentials still load because they come from instance
metadata instead of local configuration.

[`OssBuilder::credential_provider_chain`] replaces this entire chain, including
the additional AssumeRole provider. Explicit access keys,
`ALIBABA_CLOUD_*` environment credentials, `role_arn`, OIDC settings, and ECS
RAM Role settings do not override a custom chain. Set
[`reqsign_aliyun_oss::Credential::expires_in`] for temporary credentials so that
the reqsign signer can cache them and refresh them before expiration.

## Copy operations

OSS uses `CopyObject` by default. Set a copy chunk size to request server-side
multipart copy. Multipart copy reads the source metadata and uses its ETag as
an `UploadPartCopy` precondition on every part.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_oss::Oss;

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

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```

### Via Custom Credential Provider Chain

```rust,no_run
use std::time::Duration;

use opendal_core::reqsign_core;
use opendal_core::reqsign_core::time::Timestamp;
use opendal_core::reqsign_core::{Context, ProvideCredential, ProvideCredentialChain};
use opendal_service_oss::Oss;
use opendal_service_oss::reqsign_aliyun_oss::Credential;

#[derive(Debug)]
struct CustomStsProvider;

impl ProvideCredential for CustomStsProvider {
    type Credential = Credential;

    async fn provide_credential(
        &self,
        _ctx: &Context,
    ) -> reqsign_core::Result<Option<Credential>> {
        // Call your STS HTTP API here and map its response to `Credential`.
        Ok(Some(Credential {
            access_key_id: "temporary_access_key_id".to_string(),
            access_key_secret: "temporary_access_key_secret".to_string(),
            security_token: Some("temporary_security_token".to_string()),
            // The signer refreshes the credential before this timestamp.
            expires_in: Some(Timestamp::now() + Duration::from_secs(3600)),
        }))
    }
}

let chain = ProvideCredentialChain::new().push(CustomStsProvider);
let _builder = Oss::default()
    .bucket("test")
    .endpoint("https://oss-cn-beijing.aliyuncs.com")
    // This chain replaces, rather than extends, the default provider chain.
    .credential_provider_chain(chain);
```
