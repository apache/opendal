Builds a QingStor Object Storage operator using the shared S3 implementation.

The preset uses `https://s3.<zone>.qsstor.com` by default. Set `endpoint` to
override the base URL.

QingStor uses the QingCloud zone as the Signature Version 4 signing region.
Enterprise customers can set `endpoint` to the hostname supplied by QingCloud
and still specify their bucket zone. Use a QingCloud access key pair.

The preset uses path-style bucket addressing by default. Set
`enable_virtual_host_style` to choose the addressing style for an alternate
endpoint.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.

See the [QingStor Object Storage documentation](https://docsv4.qingcloud.com/user_guide/storage/object_storage/s3/intro/).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Qingstor;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Qingstor::default()
            .bucket("example-bucket")
            .zone("pek3a"),
    )
}
```
