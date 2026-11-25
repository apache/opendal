Builds an operator for IONOS Cloud Object Storage using the shared S3 implementation.

Set `region` to the code in IONOS’s endpoint table for the bucket’s ownership
model and location. The preset uses that region for signing and selects the
corresponding HTTPS S3 endpoint. For example, Frankfurt contract-owned buckets
use `eu-central-4`. Frankfurt user-owned buckets use `de` and the base URL
`https://s3.eu-central-1.ionoscloud.com`. Set `endpoint` to override the base
URL or to use a location outside the built-in table. Requests use path-style
addressing as in the IONOS s3cmd setup.

Use an IONOS Object Storage access key and secret key. See the
[endpoint tables](https://docs.ionos.com/cloud/backup-and-storage/ionos-object-storage/endpoints),
[s3cmd setup](https://docs.ionos.com/cloud/backup-and-storage/ionos-object-storage/s3-tools/s3cmd),
and [key guide](https://docs.ionos.com/cloud/backup-and-storage/ionos-object-storage/get-started/generate-key).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Ionos;

fn build_operator() -> Result<Operator> {
    Operator::new(Ionos::default().bucket("example-bucket").region("eu-central-4"))
}
```
