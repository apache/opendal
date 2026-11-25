Builds an operator for IBM Cloud Object Storage using the shared S3 implementation.

Set `endpoint` to the S3 base URL for the bucket’s storage location and network
access type (public, private, or direct). The preset uses path-style addressing
and defaults to `us-standard`, as in IBM’s HMAC signing example. Set `region`
to override the signing scope. The endpoint determines the storage location.
Set `enable_virtual_host_style` to address buckets by hostname.

Use the HMAC access key ID and secret access key for the service instance.
See IBM’s [endpoint tables](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-endpoints),
[HMAC credentials guide](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-uhc-hmac-credentials-main),
[signature example](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-hmac-signature),
and [bucket request formats](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-compatibility-api-bucket-operations).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::IbmCos;

fn build_operator() -> Result<Operator> {
    Operator::new(
        IbmCos::default()
            .bucket("example-bucket")
            .endpoint("https://s3.us.cloud-object-storage.appdomain.cloud"),
    )
}
```
