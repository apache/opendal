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

## Write verification

Pass a `WriteChecksum` intent to verify a non-append write. `Any` and `Fast`
select CRC-64/NVME. `Algo` and `Expected` support CRC-32C and CRC-64/NVME.
Successful PUT and multipart writes return the verified full-object checksum
in [`opendal_core::Metadata::checksum`]. OpenDAL verified this behavior on
SeaweedFS 4.52 with signed path-style requests.

If the gateway completes the write without a valid matching checksum, OpenDAL
returns `ErrorKind::Unexpected`. The object may already be visible. For presigned
PUTs, use `Expected` and send the signed checksum header. The HTTP client receives
the gateway's response directly.

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
