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

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
