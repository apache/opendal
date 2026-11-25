Builds an operator for IDrive e2 using the shared S3 implementation.

Set `endpoint` to the Endpoint Access URL shown with the e2 access key. Use the
matching e2 access key ID and secret key. The preset uses path-style addressing,
as in IDrive’s s3cmd setup, and supplies `us-east-1` as a signing default when
no region is specified. You can set `region` to the region selected when
creating the key.

See IDrive’s [s3cmd setup](https://www.idrive.com/s3-storage-e2/s3cmd) and
[region endpoints and access keys](https://www.idrive.com/s3-storage-e2/region-endpoints).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::IdriveE2;

fn build_operator() -> Result<Operator> {
    Operator::new(
        IdriveE2::default()
            .bucket("example-bucket")
            .endpoint("https://e2.example.com"),
    )
}
```
