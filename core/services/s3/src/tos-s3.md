# Volcengine and BytePlus TOS S3-compatible API

Enable `services-tos-s3` to use `tos-s3://bucket/prefix`. Set `endpoint` to
the product and region's **S3 Endpoint** base URL and `region` to the
bucket's region for signing. For example, `cn-beijing` uses
`https://tos-s3-cn-beijing.volces.com` for Volcengine and
`https://tos-s3-cn-beijing.bytepluses.com.cn` for BytePlus. The preset
uses AWS Signature Version 4 and virtual-hosted-style addressing.

Use the access key ID and secret issued by the corresponding product as
`access_key_id` and `secret_access_key`. The `tos` scheme selects the
native TOS API.

See the Volcengine [endpoint table](https://docs.volcengine.com/docs/torch-object-storage/regions-and-endpoints?lang=zh)
and [AWS SDK guide](https://docs.volcengine.com/docs/torch-object-storage/accessing-tos-using-awss-3-sdk?lang=zh),
and the BytePlus [endpoint table](https://docs.byteplus.com/en/docs/tos/docs-region-and-endpoint)
and [AWS SDK guide](https://docs.byteplus.com/en/docs/tos/Accessing_TOS_Using_AWS_S3_SDK).

## Example

```rust
# fn main() -> opendal_core::Result<()> {
use opendal_core::Operator;
use opendal_service_s3::TosS3;

let op = Operator::new(
    TosS3::default()
        .bucket("example-bucket")
        .root("prefix")
        .endpoint("https://tos-s3-ap-southeast-1.bytepluses.com")
        .region("ap-southeast-1")
        .access_key_id("example-access-key")
        .secret_access_key("example-secret-key"),
)?;
# Ok(())
# }
```
