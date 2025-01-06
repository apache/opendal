## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [ ] ~~write~~
- [ ] ~~create_dir~~
- [ ] ~~delete~~
- [ ] ~~copy~~
- [ ] ~~rename~~
- [x] list
- [ ] presign
- [ ] blocking

## Configuration

- `root`: Set the work directory for backend
- `endpoint`: Customizable endpoint setting

You can refer to [`IpfsBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Ipfs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Ipfs::default()
        // set the endpoint for OpenDAL
        .endpoint("https://ipfs.io")
        // set the root for OpenDAL
        .root("/ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
