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

You can refer to [`VercelArtifactsBuilder`]'s docs for more information

## Example

### Via Builder

```no_run
use anyhow::Result;
use opendal_core::services::VercelArtifacts;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = VercelArtifacts::default()
        .access_token("xxx");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
