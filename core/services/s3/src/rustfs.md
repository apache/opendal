Builds an operator for a RustFS deployment using the shared S3 implementation.

Set `endpoint` to the deployment’s S3 API URL, such as `http://127.0.0.1:9000`,
and use a RustFS access key and secret key. The preset defaults to `us-east-1`
for signing and uses path-style addressing, following the AWS SDK example. Set
`region` to override it. Enable virtual-hosted-style addressing when
`RUSTFS_SERVER_DOMAINS` is set for the endpoint.

See the [AWS SDK example](https://docs.rustfs.com/en/developer/examples/aws-sdk-go)
and [virtual-host access guide](https://docs.rustfs.com/en/integration/virtual).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

The `Rustfs` preset supports `Any` and `Fast` with RustFS 1.0.6. OpenDAL
verifies the CRC32C response for a single PUT and for each uploaded part.
A single PUT returns the verified checksum in its metadata. A multipart
write verifies individual parts and returns `None` from `Metadata::checksum()`.
The `Algo` and `Expected` intents and explicit checksums on append or
presigned writes return `Unsupported`.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Rustfs;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Rustfs::default()
            .bucket("example-bucket")
            .endpoint("http://127.0.0.1:9000"),
    )
}
```
