# Huawei Cloud OBS S3-compatible API

Enable `services-obs-s3` to use `obs-s3://bucket/prefix?region=cn-north-1`
for a Huawei Cloud OBS bucket in the China public cloud. Set `region` to
the bucket's region. OpenDAL derives the base endpoint
`https://obs.<region>.myhuaweicloud.com` and uses the same region for S3
Signature Version 4 with `UNSIGNED-PAYLOAD`.

OpenDAL uses virtual-hosted-style addressing, so requests go to
`https://<bucket>.obs.<region>.myhuaweicloud.com`. The bucket name must
be a single lowercase DNS label. To override the endpoint, set `endpoint`
to the base URL before the bucket name is added. Use `services-obs` and
`obs://` for the native OBS API.

Use Huawei Cloud-issued keys as `access_key_id` and `secret_access_key`.
For temporary credentials, pass the security token as `session_token`
along with both keys. The shared preset also reads these credentials from
the standard AWS environment variables or shared profile files.

See Huawei Cloud's [Boto3 FAQ](https://support.huaweicloud.com/obs_faq/obs_faq_0274.html)
and [Terraform S3 backend guide](https://github.com/huaweicloud/terraform-provider-huaweicloud/blob/b1d3ca6ee6e383aacfa5a82d1440d6a3fa3c6ca9/docs/guides/remote_state_backend.md).

## Example

```rust
# fn main() -> opendal_core::Result<()> {
use opendal_core::Operator;
use opendal_service_s3::ObsS3;

let op = Operator::new(
    ObsS3::default()
        .bucket("example-bucket")
        .root("prefix")
        .region("cn-north-1")
        .access_key_id("example-access-key")
        .secret_access_key("example-secret-key"),
)?;
# Ok(())
# }
```
