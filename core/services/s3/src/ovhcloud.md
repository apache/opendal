Builds a OVHcloud Object Storage operator using the shared S3 implementation.

The preset uses `https://s3.<region>.io.cloud.ovh.net` by default. Set
`endpoint` to override the base URL.

Use the lowercase bucket region for signing. Set `endpoint` for a different
deployment or to keep using a legacy `perf.cloud.ovh.net` endpoint. Use Object
Storage S3 credentials for a Public Cloud user.

The preset uses virtual-hosted-style bucket addressing by default. Set
`enable_virtual_host_style` to choose the addressing style for an alternate
endpoint.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.

See the [OVHcloud Object Storage documentation](https://docs.ovhcloud.com/en/guides/storage-and-backup/object-storage/s3-location) and the [provider's client configuration guide](https://docs.ovhcloud.com/en/guides/storage-and-backup/object-storage/s3-getting-started-with-object-storage).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Ovhcloud;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Ovhcloud::default()
            .bucket("example-bucket")
            .region("gra"),
    )
}
```
