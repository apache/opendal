Builds a DigitalOcean Spaces operator using the shared S3 implementation.

The preset uses `https://<datacenter>.digitaloceanspaces.com` by default. Set
`endpoint` to override the base URL.

Set `datacenter` to the Spaces datacenter slug, such as `nyc3`. The default
signing region is `us-east-1`, as in DigitalOcean’s AWS SDK examples. Set
`region` to override the signing region. Use a Spaces access key and secret,
issued separately from the DigitalOcean API token.

The preset uses virtual-hosted-style bucket addressing by default. Set
`enable_virtual_host_style` to choose the addressing style for an alternate
endpoint.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.

See the [DigitalOcean Spaces documentation](https://docs.digitalocean.com/products/spaces/reference/aws-sdks/) and the [provider's client configuration guide](https://docs.digitalocean.com/products/spaces/reference/s3cmd/).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Spaces;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Spaces::default()
            .bucket("example-bucket")
            .datacenter("nyc3"),
    )
}
```
