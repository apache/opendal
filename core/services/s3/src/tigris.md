Builds a Tigris operator using the shared S3 implementation.

The preset uses `https://t3.storage.dev` by default. Set `endpoint` to override
the base URL.

The signing region defaults to `auto`. Set `endpoint` or `region` for an
alternate deployment. Use an access key pair from the Tigris Console. A [forked
bucket](https://www.tigrisdata.com/docs/snapshots-and-forks/) can be used as a
regular bucket.

The preset uses virtual-hosted-style bucket addressing by default. Set
`enable_virtual_host_style` to choose the addressing style for an alternate
endpoint.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.
For [temporary credentials](https://www.tigrisdata.com/docs/iam/temporary-credentials/),
also pass the issued `session_token`.

See the [Tigris documentation](https://www.tigrisdata.com/docs/sdks/s3/).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Tigris;

fn build_operator() -> Result<Operator> {
    Operator::new(Tigris::default().bucket("example-bucket"))
}
```
