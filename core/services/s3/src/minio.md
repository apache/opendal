## Configuration

Use [`crate::MinioConfig`] for serializable configuration or this builder for
direct construction. Use [`crate::S3`] when an application needs the complete
S3 configuration surface. Applications that use the `opendal` facade enable
the `services-minio` feature.

Every MinIO deployment must provide an `endpoint`. The signing `region`
defaults to `auto`; set it explicitly when the deployment requires a configured
region.

The preset loads credentials from direct settings, standard AWS environment
variables, or static credentials in the shared AWS credential files. It does
not use AWS SSO, web identity, credential processes, ECS, EC2 metadata, or
AssumeRole. Call [`MinioBuilder::skip_signature`] for a deployment that accepts
anonymous requests.

## Write verification

Set `enable_write_checksum=true` to allow per-write checksum intents for
signed, non-append writes. The option defaults to `false`. Supply a
[`opendal_core::WriteChecksum`] intent on each write that needs verification.

OpenDAL verified signed path-style PUT and multipart uploads on MinIO
`RELEASE.2025-01-20T14-49-07Z`. Enable this option only on endpoints that
verify and return full-object CRC checksums. The repository's
`RELEASE.2024-09-22T00-33-43Z` fixture accepted incorrect CRCs. Keep the
option disabled on that fixture.

`Any` and `Fast` select CRC-64/NVME. `Algo` and `Expected` support CRC-32C
and CRC-64/NVME. Successful writes return the confirmed checksum in
[`opendal_core::Metadata::checksum`]. For a presigned PUT, use an `Expected`
checksum and send the returned signed checksum header with the request.

## Examples

Build an operator for a local MinIO deployment:

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Minio;

fn build() -> Result<Operator> {
    Operator::new(
        Minio::default()
            .bucket("data")
            .endpoint("http://127.0.0.1:9000")
            .access_key_id("minioadmin")
            .secret_access_key("minioadmin"),
    )
}
```

After registering the service, construct it from a `minio://` URI:

```rust
use opendal_core::{Operator, OperatorRegistry, Result};
use opendal_service_s3::register_minio_service;

fn build() -> Result<Operator> {
    register_minio_service(OperatorRegistry::get());
    Operator::from_uri((
        "minio://data/root",
        [("endpoint", "http://127.0.0.1:9000")],
    ))
}
```
