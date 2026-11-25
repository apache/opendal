# Alibaba Cloud OSS S3-compatible API

Enable `services-oss-s3` to use `oss-s3://bucket/prefix`. Set `region`
to the bucket's region. OpenDAL uses it to sign requests and derives
`https://s3.oss-<region>.aliyuncs.com`. OSS requires virtual-hosted-style
addressing. Set `endpoint` to override the base URL, for example with
`https://s3.oss-cn-hangzhou-internal.aliyuncs.com`. OpenDAL prepends the
bucket name to the base host.

Use an Alibaba Cloud RAM AccessKey ID and secret as `access_key_id` and
`secret_access_key`. If using issued STS credentials, pass their security
token as `session_token` along with both keys.

Alibaba Cloud requires a custom domain (CNAME) for public data API access
by new OSS users in Chinese mainland regions. The preset takes a base host.
Use the native `oss` service for a CNAME already bound to a specific bucket.
The provider's upload guide requires `x-oss-content-sha256: UNSIGNED-PAYLOAD`
and disallows `aws-chunked` encoding. The shared signer uses the standard
`x-amz-content-sha256` header for signed requests. Uploads against this
endpoint require service-side verification.

See [AWS SDK configuration](https://www.alibabacloud.com/help/en/oss/developer-reference/use-aws-sdks-to-access-oss)
and [S3 compatibility](https://www.alibabacloud.com/help/en/oss/developer-reference/compatibility-with-amazon-s3).

## Example

```rust
# fn main() -> opendal_core::Result<()> {
use opendal_core::Operator;
use opendal_service_s3::OssS3;

let op = Operator::new(
    OssS3::default()
        .bucket("example-bucket")
        .root("prefix")
        .region("cn-hangzhou")
        .access_key_id("example-access-key")
        .secret_access_key("example-secret-key"),
)?;
# Ok(())
# }
```
