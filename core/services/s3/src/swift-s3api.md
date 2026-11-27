# OpenStack Swift s3api

Enable `services-swift-s3api` to use `swift-s3api://container/prefix`.
Set `endpoint` to the base URL of the Swift proxy with `s3api` enabled.
The URI authority selects a Swift container. OpenDAL defaults to path-style
addressing and the `us-east-1` signing region. Set `region` to match the
middleware's `location` if the deployment specifies a different value.
Set `enable_virtual_host_style=true` when the deployment configures
`storage_domain` and matching DNS.

For Keystone, use the access key and secret returned by
`openstack ec2 credentials create`. For TempAuth, use `account:user` and
the configured user password. Set these as `access_key_id` and
`secret_access_key`. The `swift` scheme selects the native Swift API,
which uses the Swift account URL and token.

See the [s3api middleware](https://docs.openstack.org/swift/latest/middleware.html#aws-s3-api)
and [Swift proxy configuration](https://github.com/openstack/swift/blob/master/etc/proxy-server.conf-sample).

## Write checksums

Checksum writes require Swift 2.38.3 or later with `s3api` enabled. Use
`WriteChecksum::Any` or `WriteChecksum::Fast` on signed non-append writes.
Swift verifies CRC32C on each `PutObject` and `UploadPart` request. OpenDAL
reports a rejected CRC32C as `ErrorKind::ChecksumMismatch`.

Multipart completion sends ordered part numbers and ETags. Successful writes
leave `Metadata::checksum()` empty. Explicit `Algo` and `Expected` intents,
append with a checksum, and presigned writes with a checksum return
`Unsupported` before I/O.

## Example

```rust
# fn main() -> opendal_core::Result<()> {
use opendal_core::Operator;
use opendal_service_s3::SwiftS3api;

let op = Operator::new(
    SwiftS3api::default()
        .bucket("example-container")
        .root("prefix")
        .endpoint("https://swift.example.com")
        .access_key_id("example-access-key")
        .secret_access_key("example-secret-key"),
)?;
# Ok(())
# }
```
