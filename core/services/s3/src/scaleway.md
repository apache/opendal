Builds a Scaleway Object Storage operator using the shared S3 implementation.

The preset uses `https://s3.<region>.scw.cloud` by default. Set `endpoint` to
override the base URL.

Set `region` to the bucket’s Scaleway region. It is also the signing region.
Use a Scaleway API access key and secret key.

The preset uses path-style bucket addressing by default. Set
`enable_virtual_host_style` to choose the addressing style for an alternate
endpoint.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.

See Scaleway’s [client configuration](https://github.com/scaleway/docs-content/blob/main/pages/object-storage/api-clients/cli-tools-and-clients.mdx)
and [endpoint formats](https://github.com/scaleway/docs-content/blob/main/pages/object-storage/concepts/glossary.mdx#endpoint).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Scaleway;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Scaleway::default()
            .bucket("example-bucket")
            .region("nl-ams"),
    )
}
```
