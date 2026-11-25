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
