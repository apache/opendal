# lakeFS S3 Gateway

Enable `services-lakefs-s3` to use `lakefs-s3://example-repo/dev/data`.
Set `endpoint` to the base URL of the lakeFS S3 Gateway. The URI authority
selects the repository as an S3 bucket. The root starts with an explicit
lakeFS reference, such as a branch name or commit ID. In the example,
reading `file` requests the S3 key `dev/data/file` in `example-repo`.
For a root at the top of a branch, use just the branch, as in
`lakefs-s3://example-repo/dev`. Use a writable branch for writes. On a
gateway with tenant resolution, use its composite bucket name, such as
`team-a--example-repo`.

OpenDAL defaults to path-style addressing and the `us-east-1` signing
region. Override the signing region with `region` to match
`gateways.s3.region` and set `enable_virtual_host_style=true` when the
gateway and DNS are configured for virtual-hosted-style requests. Use
lakeFS-issued `access_key_id` and `secret_access_key`. With issued lakeFS
Enterprise temporary credentials, also set `session_token`.

The gateway supports a subset of S3 operations. The `lakefs` scheme
selects the native API, with the endpoint in the URI authority and the
repository and reference in the path.

See [S3 object and prefix examples](https://docs.lakefs.io/reference/python/boto/),
[gateway configuration](https://docs.lakefs.io/reference/configuration/),
[virtual-host addressing](https://docs.lakefs.io/admin/virtual-host-addressing/),
[temporary credentials](https://docs.lakefs.io/security/s3-temporary-credentials/),
and [supported operations](https://docs.lakefs.io/reference/s3/).

## Source ETag conditions

The preset reports the capabilities of the shared S3 implementation. With
local storage, lakeFS 1.88.0 can accept a stale source ETag on a ranged
`GetObject` or `UploadPartCopy` request and return or copy the current bytes.
This affects both reads and `Writer::copy_from_options`.

To reject these calls before sending a request, set `read_with_if_match`
to `false` on the operator. Reads and writer copies that specify `if_match`
then return `ErrorKind::Unsupported`.

```rust
# fn guard(op: opendal_core::Operator) -> opendal_core::Result<opendal_core::Operator> {
use opendal_core::layers::CapabilityOverrideLayer;

let op = op.layer(CapabilityOverrideLayer::from_overrides(
    "read_with_if_match=false",
)?);
# Ok(op)
# }
```

## Example

```rust
# fn main() -> opendal_core::Result<()> {
use opendal_core::Operator;
use opendal_service_s3::LakefsS3;

let op = Operator::new(
    LakefsS3::default()
        .bucket("example-repo")
        .root("dev/data")
        .endpoint("https://lakefs.example.com")
        .access_key_id("example-access-key")
        .secret_access_key("example-secret-key"),
)?;
# Ok(())
# }
```
