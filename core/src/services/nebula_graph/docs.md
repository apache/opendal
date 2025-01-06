## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [x] list
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `root`: Set the working directory of `OpenDAL`
- `host`: Set the host address of NebulaGraph's graphd server
- `port`: Set the port of NebulaGraph's graphd server
- `username`: Set the username of NebulaGraph's graphd server
- `password`: Set the password of NebulaGraph's graphd server
- `space`: Set the passspaceword of NebulaGraph
- `tag`: Set the tag of NebulaGraph
- `key_field`: Set the key_field of NebulaGraph
- `value_field`: Set the value_field of NebulaGraph

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::NebulaGraph;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = NebulaGraph::default();
    builder.root("/");
    builder.host("127.0.0.1");
    builder.port(9669);
    builder.space("your_space");
    builder.tag("your_tag");
    // key field type in the table should be compatible with Rust's &str like text
    builder.key_field("key");
    // value field type in the table should be compatible with Rust's Vec<u8> like bytea
    builder.value_field("value");

    let op = Operator::new(builder)?.finish();
    Ok(())
}
```
