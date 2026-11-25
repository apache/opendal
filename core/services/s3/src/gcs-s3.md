# Google Cloud Storage XML API

Enable `services-gcs-s3` to use `gcs-s3://bucket/prefix`. OpenDAL defaults
to `https://storage.googleapis.com`, AWS Signature Version 4 with signing
region `auto`, and path-style addressing. Override the base URL with
`endpoint` and the signing region with `region`. Set
`enable_virtual_host_style=true` for buckets whose names fit in a single
DNS label. Use path-style addressing for bucket names that contain dots.

Use a Cloud Storage HMAC access ID and secret as `access_key_id` and
`secret_access_key`. The `gcs` scheme selects the native JSON API and
supports Google Cloud credentials such as Application Default Credentials.

See [migration from Amazon S3](https://cloud.google.com/storage/docs/aws-simple-migration),
[XML API endpoints](https://cloud.google.com/storage/docs/request-endpoints#xml-api),
and [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys).

## Example

```rust
# fn main() -> opendal_core::Result<()> {
use opendal_core::Operator;
use opendal_service_s3::GcsS3;

let op = Operator::new(
    GcsS3::default()
        .bucket("example-bucket")
        .root("prefix")
        .access_key_id("example-access-key")
        .secret_access_key("example-secret-key"),
)?;
# Ok(())
# }
```
