Builds an operator for Ceph RADOS Gateway using the shared S3 implementation.

Set `endpoint` to the RADOS Gateway S3 API base URL. The preset uses
path-style addressing and defaults to a signing region of `us-east-1`. Set
`region` to match your deployment. Enable virtual-hosted-style addressing when
your deployment has a matching `rgw_dns_name` or zonegroup hostname and the
required DNS and TLS configuration.

Use the S3 access and secret keys of a gateway user. See the Ceph guides for
[bucket addressing](https://docs.ceph.com/en/latest/radosgw/s3/commons/#bucket-and-host-name),
[SigV4 authentication](https://docs.ceph.com/en/latest/radosgw/s3/authentication/#aws-signature-v4),
and [Python S3 examples](https://docs.ceph.com/en/latest/radosgw/s3/python/).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

The [RGW authentication guide](https://docs.ceph.com/en/latest/radosgw/s3/authentication/#presigned-urls)
describes a deployment option that disables presigned URL authentication.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::CephRgw;

fn build_operator() -> Result<Operator> {
    Operator::new(
        CephRgw::default()
            .bucket("example-bucket")
            .endpoint("https://rgw.example.com"),
    )
}
```
