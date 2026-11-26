Builds an operator for a SeaweedFS S3 gateway using the shared S3 implementation.

Set `endpoint` to the deployment’s S3 gateway URL, such as
`http://127.0.0.1:8333`, and use a key pair configured for the gateway. The
preset defaults to path-style addressing and a signing region of `us-east-1`.
Set `region` to override the signing scope. Enable virtual-hosted-style
addressing only when the gateway’s `domainName` includes the endpoint host.

See the [S3 API guide](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API),
[credential guide](https://github.com/seaweedfs/seaweedfs/wiki/S3-Credentials),
[gateway options](https://github.com/seaweedfs/seaweedfs/blob/master/weed/command/s3.go),
and [CLI signing example](https://github.com/seaweedfs/seaweedfs/wiki/Helm-Chart-Recipes).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

Set `enable_write_checksum` to true for an S3 gateway that verifies and returns
full-object CRC checksums for PUT and multipart uploads. The option defaults to
false and applies only to writes. OpenDAL verified this behavior with SeaweedFS
4.52 using signed path-style requests. Check compatibility at your endpoint
before enabling this option on other deployments. With
`ChecksumLayer::require_read(true)`, full-object reads on this preset return
`Unsupported`.

`WriteChecksum::Any` and `WriteChecksum::Fast` use CRC-64/NVME. Explicit algorithm
and expected checksum intents support CRC-32C and CRC-64/NVME. Successful writes
return the verified checksum in [`opendal_core::Metadata::checksum`].

SeaweedFS does not support appending to objects through the S3 API. Disable
`write_can_append` if your application uses it. Enable versioning on the bucket
before relying on version-aware operations. For an unsigned gateway, use the
generic [`S3`](crate::S3) builder’s anonymous request options.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Seaweedfs;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Seaweedfs::default()
            .bucket("example-bucket")
            .endpoint("http://127.0.0.1:8333"),
    )
}
```
