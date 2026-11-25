Builds a Supabase Storage operator using the shared S3 implementation.

The preset uses `https://<project_ref>.storage.supabase.co/storage/v1/s3` by
default. Set `endpoint` to override the base URL.

Set `region` to the region shown in the project’s S3 settings. For local
Supabase, set `endpoint` to `http://127.0.0.1:54321/storage/v1/s3` and `region`
to `local`.

Generated S3 access keys are for server-side use and bypass Row Level Security
(RLS). To use the caller’s RLS policies, set `access_key_id` to the project
reference, `secret_access_key` to the project’s legacy `anon` key, and
`session_token` to the user’s JWT. Supply a fresh JWT when it expires.

The preset uses path-style bucket addressing. Enable the S3 protocol in the
project's Storage settings for S3 access. Supabase does not support S3 object
versioning or restoring deleted objects. See [S3 compatibility](https://supabase.com/docs/guides/storage/s3/compatibility).

Pass the access key and secret key together, or use the standard AWS credential
environment variables or shared credentials file with the provider’s keys.

See the [Supabase Storage documentation](https://supabase.com/docs/guides/storage/s3/authentication).

```rust
use opendal_core::{Operator, Result};
use opendal_service_s3::Supabase;

fn build_operator() -> Result<Operator> {
    Operator::new(
        Supabase::default()
            .bucket("example-bucket")
            .project_ref("example-project")
            .region("us-west-1"),
    )
}
```
