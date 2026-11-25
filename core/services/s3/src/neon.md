Builds an operator for Neon Object Storage using the shared S3 implementation.

Set `endpoint` to the S3 URL of the branch you want to access. The Neon Console
and `neon env pull` provide it as `AWS_ENDPOINT_URL_S3`. The preset takes the
signing region from a standard branch URL such as
`https://<branch_id>.storage.<cell>.<region>.aws.neon.tech`. Use `region` to
override it. A custom endpoint requires an explicit region. Requests use
path-style addressing and SigV4.

Use the token ID and S3 secret access key of a Neon credential with the
`storage:read` or `storage:write` scopes required by the application. A child
branch inherits the parent’s objects and buckets. Credentials from the parent
also work on the child. Set the child’s endpoint to target its copy of the data.

See [Neon’s connection guide](https://neon.com/docs/storage/get-started) and
[S3 compatibility reference](https://neon.com/docs/storage/s3-compatibility).

Pass the provider’s access key ID and secret access key together, or supply them
through the AWS credential environment variables or shared credentials file.

Neon does not enforce object versioning and returns a `null` version ID. It
also replaces the object on append. Disable the versioning, restore, and
`write_can_append` capabilities if your application needs those guarantees.

Neon ignores conditional headers on `CopyObject`, `DeleteObject`, and
`CompleteMultipartUpload`. Single-part conditional writes lack atomicity across
concurrent writers. Disable `copy_with_if_not_exists`, `copy_with_if_match`,
`delete_with_if_match`, `write_with_if_not_exists`, and `write_with_if_match`
if your application relies on those guarantees.

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Neon;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Neon::default()
            .bucket("example-bucket")
            .endpoint("https://br-example.storage.c-6.us-east-2.aws.neon.tech"),
    )
}
```
