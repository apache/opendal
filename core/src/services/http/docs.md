## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [ ] ~~write~~
- [ ] ~~create_dir~~
- [ ] ~~delete~~
- [ ] ~~copy~~
- [ ] ~~rename~~
- [ ] ~~list~~
- [ ] ~~presign~~
- [ ] blocking

## Notes

Only `read` and `stat` are supported. We can use this service to visit any
HTTP Server like nginx, caddy.

## Configuration

- `endpoint`: set the endpoint for http
- `root`: Set the work directory for backend

You can refer to [`HttpBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Http;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create http backend builder
    let mut builder = Http::default().endpoint("127.0.0.1");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
