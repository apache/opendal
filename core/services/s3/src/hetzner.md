Builds a Hetzner Object Storage operator using the shared S3 implementation.

The preset uses `https://<location>.your-objectstorage.com` by default. Set
`endpoint` to override the base URL.

The signing region defaults to `location`, following Hetzner’s S3 client
examples, and can be set independently with `region`. With a custom endpoint,
supply either `location` or `region`. S3 credentials belong to the Hetzner
project and can be used across its locations.

The preset uses virtual-hosted-style bucket addressing by default. Set
`enable_virtual_host_style` to choose the addressing style for an alternate
endpoint.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.

See the [Hetzner Object Storage documentation](https://docs.hetzner.com/storage/object-storage/getting-started/using-s3-api-tools/).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Hetzner;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Hetzner::default()
            .bucket("example-bucket")
            .location("fsn1"),
    )
}
```
