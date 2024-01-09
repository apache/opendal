## Capabilities

This service can be used to:

- [ ] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] ~~copy~~
- [ ] ~~rename~~
- [ ] ~~list~~
- [ ] ~~scan~~
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `access_token`: set the access_token for Rest API

You can refer to [`VercelArtifactsBuilder`]'s docs for more information

## Example

### Via Builder

```rust
use anyhow::Result;
use opendal::services::VercelArtifacts;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = VercelArtifacts::default();

    builder.access_token("xxx");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
