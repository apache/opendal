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

## Write verification

Pass a `WriteChecksum` intent to verify a non-append write. `Any` and `Fast`
select CRC-32C. `Algo` and `Expected` accept CRC-32C only. RGW can commit a
multipart upload with an incorrect full-object CRC-64/NVME checksum.
Successful PUT and multipart writes return the verified full-object CRC-32C
in `Metadata::checksum()`. OpenDAL verified CRC-32C on RGW 20.2.5 and 21.2.0
with signed path-style requests.

RGW 18.2.8 and 19.2.7 can store an object without checking or returning its
CRC-32C checksum. If RGW completes a write without a valid matching checksum,
OpenDAL returns `ErrorKind::Unexpected`. The object may already be visible.
For presigned PUTs, use `Expected` and send both signed checksum headers. The
HTTP client receives the gateway's response directly.

## Source ETag conditions in writer copies

On Ceph RGW 20.2.5, a bounded native `UploadPartCopy` can accept a stale
source ETag passed to `Writer::copy_from_options` as `if_match`.
`Writer::close` can then succeed and commit bytes from the current source.
On the same RGW version, a ranged `GetObject` with the stale `If-Match`
returns HTTP 412.

For an affected deployment, disable native writer copies on the operator to
stream the source bytes through the client. The streaming path sends
`If-Match` with the source read and returns `ErrorKind::ConditionNotMatch`
if RGW rejects it. The override applies to all writer copies on the layered
operator, including unconditional copies. Conditional reads remain available,
and operators without the layer can still use native writer copies when
eligible.

```rust
# fn use_streaming_copies(op: opendal_core::Operator) -> opendal_core::Result<opendal_core::Operator> {
use opendal_core::layers::CapabilityOverrideLayer;

let op = op.layer(CapabilityOverrideLayer::from_overrides(
    "write_can_copy_from=false",
)?);
# Ok(op)
# }
```

## Example

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
