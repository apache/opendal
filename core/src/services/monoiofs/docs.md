## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] append
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [ ] list
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `root`: Set the work dir for backend.

You can refer to [`MonoiofsBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Monoiofs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create monoiofs backend builder.
    let mut builder = Monoiofs::default()
        // Set the root for monoiofs, all operations will happen under this root.
        //
        // NOTE: the root must be absolute path.
        .root("/tmp");

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
