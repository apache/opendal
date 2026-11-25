Builds a Wasabi Object Storage operator using the shared S3 implementation.

The preset selects a regional service URL from Wasabi’s published list. For
instance, `eu-west-2` uses `https://s3.eu-west-2.wasabisys.com`. Set `endpoint`
to use a published alias such as `https://s3.fr-1.wasabisys.com`, or to use a
region added after the preset’s list was last updated. Set `region` to the
bucket’s Wasabi region for signing. Use a Wasabi access key and secret key.

The preset uses path-style bucket addressing by default. Set
`enable_virtual_host_style` to choose the addressing style for an alternate
endpoint.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.
For [temporary STS credentials](https://docs.wasabi.com/apidocs/iam-and-sts-support),
also pass the issued `session_token`.

See the [Wasabi Object Storage documentation](https://docs.wasabi.com/docs/what-are-the-service-urls-for-wasabi-s-different-storage-regions) and the [provider's client configuration guide](https://docs.wasabi.com/docs/how-do-i-use-aws-sdk-for-java-v2-with-wasabi).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Wasabi;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Wasabi::default()
            .bucket("example-bucket")
            .region("eu-west-2"),
    )
}
```
