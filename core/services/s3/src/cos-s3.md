# Tencent Cloud COS S3-compatible API

Enable `services-cos-s3` to use `cos-s3://example-bucket-1250000000/prefix`.
Use the full bucket name, including its numeric APPID suffix. Set `region`
to the COS bucket's region. OpenDAL uses it to sign requests and derives
`https://cos.<region>.myqcloud.com`. The preset uses virtual-hosted-style
addressing. Set `endpoint` to override the base URL, leaving the bucket name
out of the endpoint host.

Use a Tencent Cloud `SecretId` and `SecretKey` as `access_key_id` and
`secret_access_key`. If using issued STS credentials, supply the token as
`session_token` with the temporary key pair. The `cos` and `cosn` schemes
select the native COS API.

See [S3 support](https://cloud.tencent.com/document/product/436/41284),
[AWS SDK configuration](https://www.tencentcloud.com/document/product/436/32537),
and [COS regions and endpoints](https://www.tencentcloud.com/document/product/436/6224).

## Example

```rust
# fn main() -> opendal_core::Result<()> {
use opendal_core::Operator;
use opendal_service_s3::CosS3;

let op = Operator::new(
    CosS3::default()
        .bucket("example-bucket-1250000000")
        .root("prefix")
        .region("ap-guangzhou")
        .access_key_id("example-access-key")
        .secret_access_key("example-secret-key"),
)?;
# Ok(())
# }
```
