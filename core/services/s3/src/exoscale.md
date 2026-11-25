Builds an Exoscale Simple Object Storage operator using the shared S3 implementation.

The preset uses `https://sos-<zone>.exo.io` by default. Set `endpoint` to
override the base URL.

The zone also supplies the signing region. Use an Exoscale API key authorized
for Simple Object Storage.

The preset uses path-style bucket addressing by default. Set
`enable_virtual_host_style` to choose the addressing style for an alternate
endpoint.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.

See the [Exoscale Simple Object Storage documentation](https://community.exoscale.com/product/storage/object-storage/quick-start/) and the [provider's client configuration guide](https://community.exoscale.com/product/storage/object-storage/how-to/awscli/).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Exoscale;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Exoscale::default()
            .bucket("example-bucket")
            .zone("ch-dk-2"),
    )
}
```
