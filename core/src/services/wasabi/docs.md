## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [x] scan
- [x] presign
- [ ] blocking

## Configuration

- `root`: Set the work dir for backend.
- `bucket`: Set the container name for backend.
- `endpoint`: Set the endpoint for backend.
- `region`: Set the region for backend.
- `access_key_id`: Set the access_key_id for backend.
- `secret_access_key`: Set the secret_access_key for backend.
- `security_token`: Set the security_token for backend.
- `default_storage_class`: Set the default storage_class for backend.
- `server_side_encryption`: Set the server_side_encryption for backend.
- `server_side_encryption_aws_kms_key_id`: Set the server_side_encryption_aws_kms_key_id for backend.
- `server_side_encryption_customer_algorithm`: Set the server_side_encryption_customer_algorithm for kend.
- `server_side_encryption_customer_key`: Set the server_side_encryption_customer_key for backend.
- `server_side_encryption_customer_key_md5`: Set the server_side_encryption_customer_key_md5 for kend.
- `disable_config_load`: Disable aws config load from env
- `enable_virtual_host_style`: Enable virtual host style.

Refer to [`WasabiBuilder`]'s public API docs for more information.

### Temporary security credentials

OpenDAL now provides support for S3 temporary security credentials in IAM.

The way to take advantage of this feature is to build your S3 backend with `Builder::security_token`.

But OpenDAL will not refresh the temporary security credentials, please keep in mind to refresh those entials in time.

### Server Side Encryption

OpenDAL provides full support of S3 Server Side Encryption(SSE) features.

The easiest way to configure them is to use helper functions like

- SSE-KMS: `server_side_encryption_with_aws_managed_kms_key`
- SSE-KMS: `server_side_encryption_with_customer_managed_kms_key`
- SSE-S3: `server_side_encryption_with_s3_key`
- SSE-C: `server_side_encryption_with_customer_key`

If those functions don't fulfill need, low-level options are also provided:

- Use service managed kms key
  - `server_side_encryption="aws:kms"`
- Use customer provided kms key
  - `server_side_encryption="aws:kms"`
  - `server_side_encryption_aws_kms_key_id="your-kms-key"`
- Use S3 managed key
  - `server_side_encryption="AES256"`
- Use customer key
  - `server_side_encryption_customer_algorithm="AES256"`
  - `server_side_encryption_customer_key="base64-of-your-aes256-key"`
  - `server_side_encryption_customer_key_md5="base64-of-your-aes256-key-md5"`

After SSE have been configured, all requests send by this backed will attach those headers.

Reference: [Protecting data using server-side encryption](https://docs.aws.amazon.com/AmazonS3/latest/rguide/serv-side-encryption.html)

## Example

### Via Builder

#### Basic Setup

```rust
use anyhow::Result;
use opendal::services::Wasabi;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create s3 backend builder.
    let mut builder = Wasabi::default();
    // Set the root for s3, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root("/path/to/dir");
    // Set the bucket name, this is required.
    builder.bucket("test");
    // Set the endpoint.
    //
    // For examples:
    // - "https://s3.wasabisys.com"
    // - "http://127.0.0.1:9000"
    // - "https://oss-ap-northeast-1.aliyuncs.com"
    // - "https://cos.ap-seoul.myqcloud.com"
    //
    // Default to "https://s3.wasabisys.com"
    builder.endpoint("https://s3.wasabisys.com");
    // Set the access_key_id and secret_access_key.
    //
    // OpenDAL will try load credential from the env.
    // If credential not set and no valid credential in env, OpenDAL will
    // send request without signing like anonymous user.
    builder.access_key_id("access_key_id");
    builder.secret_access_key("secret_access_key");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```

#### Wasabi with SSE-C

```rust
use anyhow::Result;
use opendal::services::Wasabi;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Wasabi::default();

    // Enable SSE-C
    builder.server_side_encryption_with_customer_key("AES256", "customer_key".as_bytes());

    let op = Operator::new(builder)?.finish();

    Ok(())
}
```

#### Wasabi with SSE-KMS and aws managed kms key

```rust
use anyhow::Result;
use opendal::services::Wasabi;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Wasabi::default();

    // Enable SSE-KMS with aws managed kms key
    builder.server_side_encryption_with_aws_managed_kms_key();

    let op = Operator::new(builder)?.finish();

    Ok(())
}
```

#### Wasabi with SSE-KMS and customer managed kms key

```rust
use anyhow::Result;
use opendal::services::Wasabi;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Wasabi::default();

    // Enable SSE-KMS with customer managed kms key
    builder.server_side_encryption_with_customer_managed_kms_key("aws_kms_key_id");

    let op = Operator::new(builder)?.finish();

    Ok(())
}
```

#### Wasabi with SSE-S3

```rust
use anyhow::Result;
use log::info;
use opendal::services::Wasabi;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Wasabi::default();

    // Enable SSE-S3
    builder.server_side_encryption_with_s3_key();

    let op = Operator::new(builder)?.finish();

    Ok(())
}
```