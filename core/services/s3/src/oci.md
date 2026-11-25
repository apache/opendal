Builds an Oracle Cloud Infrastructure Object Storage operator using the shared
S3 implementation.

The preset uses
`https://<namespace>.compat.objectstorage.<region>.oci.customer-oci.com` by
default. Set `endpoint` to override the base URL.

The derived endpoint uses the commercial OC1 realm. Set `endpoint` to the
realm’s path-style S3 Compatibility API URL for other realms or when using an
existing `oraclecloud.com` endpoint. The OCI region is also the signing region.
Use the access key and secret from a Customer Secret Key. OCI does not support
object-level ACLs and requires Signature Version 4.

The preset uses path-style bucket addressing. To use OCI's virtual-hosted-style
endpoint, set `https://vhcompat.objectstorage.<region>.oci.customer-oci.com`
on the [generic S3 builder](crate::S3) and enable virtual-hosted-style
addressing. OCI organizes buckets into compartments and encrypts objects at
rest. Its IAM policies govern access to objects.

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.

See the [S3 Compatibility API](https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/s3compatibleapi.htm)
and [OCI regions and realms](https://docs.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Oci;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Oci::default()
            .bucket("example-bucket")
            .namespace("example-namespace")
            .region("us-ashburn-1"),
    )
}
```
