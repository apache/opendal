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

## Read and stat checksums

For signed requests to standard AWS S3 endpoints, use
[`opendal_core::Operator::stat_with`] or [`opendal_core::Operator::reader_with`]
with `.checksum(true)` to request stored full-object checksums. Stat returns the
metadata directly. A reader that observes a full-object response first exposes
its metadata through [`opendal_core::Reader::metadata`].
[`opendal_core::Operator::read_with`] returns the bytes.

OpenDAL exposes full-object CRC-64/NVME, CRC-32C, and SHA-256 checksums when S3
returns them. A ranged read can expose a checksum when the response covers the
whole object. Composite checksums and responses that cover only part of the
object leave [`opendal_core::Metadata::checksum`] empty. OpenDAL omits checksum mode by default
and for presigned requests, anonymous access, directory buckets, and
S3-compatible endpoints and presets.

## Object restoration

S3 restoration requires bucket versioning. [`opendal_core::Operator::restore`]
removes the current delete marker. Calling it on an already-live object succeeds.
It does not scan or remove older marker versions. If no live object or current
delete marker exists, it returns [`opendal_core::ErrorKind::NotFound`].

Use [`opendal_core::Operator::restore_with`] to promote a specific version:

```rust,no_run
# use opendal_core::Operator;
# use opendal_core::Result;
# async fn restore(op: Operator, version: &str) -> Result<()> {
op.restore_with("path/to/file")
    .version(version)
    .await?;
# Ok(())
# }
```

Add `.if_not_exists(true)` when a selected version must not overwrite an
object recreated by another writer. This condition requires an explicit
version.

## Configuration

Use [`crate::S3Config`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## AWS container credentials

The default AWS credential chain already reads ambient container credential
settings. The container credential options expose the same provider settings
through typed builder methods, serialized configuration, and URI options. Setting
any of these options replaces the ECS slot in the default chain without changing
that slot's position.

Endpoint selection follows this order:

1. `container_credentials_endpoint`
2. `container_credentials_relative_uri`
3. `AWS_CONTAINER_CREDENTIALS_FULL_URI`
4. `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`

`container_metadata_uri_override` changes the base URI for relative endpoints
only and does not select an endpoint by itself. Authorization token selection
follows this order:

1. `container_authorization_token`
2. `container_authorization_token_file`
3. `AWS_CONTAINER_AUTHORIZATION_TOKEN`
4. `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`

These options configure the existing ECS slot; they do not force it ahead of
environment, profile, SSO, web identity, or external process providers.
Configured static credentials retain their higher priority. [`crate::S3::role_arn`]
continues to use the assembled provider as the source for role assumption, and
[`crate::S3::credential_provider_chain`] continues to replace the assembled
chain. [`crate::S3::disable_config_load`] disables automatic endpoint loading and
the environment, profile, SSO, web-identity, and external process credential
slots. Region environment lookup and compute metadata credential providers,
including container credentials and EC2 instance metadata, remain enabled.

## S3 Express One Zone

OpenDAL recognizes valid AWS directory bucket names on AWS endpoints and
automatically uses the bucket's virtual-hosted zonal endpoint. Set the bucket
name and Region to connect with your configured AWS credentials.

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

fn build_operator() -> Result<Operator> {
    Operator::new(
        S3::default()
            .bucket("example--usw2-az1--x-s3")
            .region("us-west-2"),
    )
}
```

This automatic behavior applies only to AWS endpoints. S3-compatible services
retain the existing IAM authentication behavior.

In the standard AWS partition, the zonal endpoint has the form
`https://<bucket>.s3express-<zone-id>.<region>.amazonaws.com`. You can omit the
endpoint or supply a matching AWS regional or zonal endpoint. OpenDAL also
accepts `s3.amazonaws.com` in the standard AWS partition. OpenDAL rejects
mismatched or unsupported AWS endpoints, including dual-stack endpoints, with
[`opendal_core::ErrorKind::ConfigInvalid`].

Grant `s3express:CreateSession` on the bucket and allow `ReadWrite` sessions.
OpenDAL uses the configured AWS credentials to call `CreateSession` and refreshes
sessions automatically. It signs `CopyObject`, `UploadPartCopy`, and presigned
requests with the underlying IAM credentials.
[See the AWS `CreateSession` API reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateSession.html).

### Directory bucket behavior

- **Listing.** By default, OpenDAL uses `ListObjectsV2` with `/` as the delimiter.
  AWS accepts only `/` as a delimiter and requires nonempty request prefixes to
  end in `/`.
  OpenDAL's default operator filters directory listings for partial prefixes and
  walks directories for recursive listings, which can require additional requests.
  Leave `disable_list_objects_v2` unset. Directory buckets do not support
  `start_after` or guarantee lexicographical order.
  [See the AWS `ListObjectsV2` API reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).
- **Versioning and restore.** AWS directory buckets do not support S3 Versioning.
  OpenDAL disables version-specific stat, read, delete, and copy-source options,
  listing versions or delete markers, and `restore`.
  [See the AWS `PutBucketVersioning` API reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html).
- **Writes.** OpenDAL supports conditional writes with `If-Match` and
  `If-None-Match: *`, as well as appending to objects in the `EXPRESS_ONEZONE`
  storage class. The service does not advertise `write_with_content_encoding`
  for directory buckets.
  [See the AWS `PutObject` API reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).

## Temporary security credentials

OpenDAL now provides support for S3 temporary security credentials in IAM.

The way to take advantage of this feature is to build your S3 backend with `Builder::session_token`.

But OpenDAL will not refresh the temporary security credentials, please keep in mind to refresh those credentials in time.

## Server Side Encryption

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

Reference: [Protecting data using server-side encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html)

## Example

## Via Builder

### Basic Setup

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    // Create s3 backend builder.
    let mut builder = S3::default()
      // Set the root for s3, all operations will happen under this root.
      //
      // NOTE: the root must be absolute path.
      .root("/path/to/dir")
      // Set the bucket name. This is required.
      .bucket("test")
      // Set the signing region. Compatible services define their own region rules.
      .region("us-east-1")
      // Set the endpoint.
      //
      // For examples:
      // - "https://s3.amazonaws.com"
      // - "http://127.0.0.1:9000"
      // - "https://oss-ap-northeast-1.aliyuncs.com"
      // - "https://cos.ap-seoul.myqcloud.com"
      //
      // Default to "https://s3.amazonaws.com"
      .endpoint("https://s3.amazonaws.com")
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

### S3 with SSE-C

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable SSE-C
      .server_side_encryption_with_customer_key("AES256", "customer_key".as_bytes());

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // Writing your testing code here.

    Ok(())
}
```

### S3 with SSE-KMS and aws managed kms key

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      // Setup builders
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable SSE-KMS with aws managed kms key
      .server_side_encryption_with_aws_managed_kms_key();

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // Writing your testing code here.

    Ok(())
}
```

### S3 with SSE-KMS and customer managed kms key

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      // Setup builders
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable SSE-KMS with customer managed kms key
      .server_side_encryption_with_customer_managed_kms_key("aws_kms_key_id");

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // Writing your testing code here.

    Ok(())
}
```

### S3 with SSE-S3

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      // Setup builders
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable SSE-S3
      .server_side_encryption_with_s3_key();

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // Writing your testing code here.

    Ok(())
}
```

### S3 with default ACL

```rust,no_run
use log::info;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_s3::S3;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = S3::default()
      // Setup builders
      .root("/path/to/dir")
      .bucket("test")
      .region("us-east-1")
      .endpoint("https://s3.amazonaws.com")
      .access_key_id("access_key_id")
      .secret_access_key("secret_access_key")
      // Enable public-read ACL
      .default_acl("public-read");

    let op = Operator::new(builder)?;
    info!("operator: {:?}", op);

    // New objects will be created with public-read ACL

    Ok(())
}
```
