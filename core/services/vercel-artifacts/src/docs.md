## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [ ] delete
- [ ] ~~list~~
- [ ] ~~copy~~
- [ ] ~~rename~~
- [ ] ~~presign~~

## Limitations

Vercel Remote Cache stores build artifacts addressed by a hash of the task inputs.
Because of its append-only, hash-keyed design, it has the following limitations:
- **Folder Operations**: It does not support creating directories (`create_dir`) or listing files (`list`).
- **Resource Deletion**: It does not support deleting individual remote cache artifacts (`delete`). Cache invalidation is handled automatically by Vercel or locally via input changes (which produce a new task hash).
- **Suffix Range Reads**: `read_with_suffix` is not declared because suffix range reads (`Range: bytes=-N`) have not been verified against the Vercel Remote Cache API. Full reads and standard range reads (`Range: bytes=X-Y`) are supported.

## Configuration

- `access_token`: set the access_token for Rest API
- `endpoint`: set the API endpoint (default: `https://api.vercel.com`)
- `team_id`: optional Vercel team ID, appended as `teamId` query parameter
- `team_slug`: optional Vercel team slug, appended as `slug` query parameter

You can refer to [`VercelArtifactsBuilder`]'s docs for more information

## Example

### Via Builder

```no_run
use anyhow::Result;
use opendal_service_vercel_artifacts::VercelArtifacts;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = VercelArtifacts::default()
        .access_token("xxx")
        .endpoint("https://my-vercel-api.example.com")
        .team_id("team_xxx");

    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```
