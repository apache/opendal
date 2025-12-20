## Capabilities

This service can be used to:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [x] rename
- [ ] presign

## Configuration

- `root`: Set the work directory for backend
- `endpoint`: Koofr endpoint
- `email` Koofr email
- `password` Koofr password

You can refer to [`KoofrBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_koofr::Koofr;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Koofr::default()
        // set the storage bucket for OpenDAL
        .root("/")
        // set the bucket for OpenDAL
        .endpoint("https://api.koofr.net/")
        // set the email for OpenDAL
        .email("me@example.com")
        // set the password for OpenDAL
        .password("xxx xxx xxx xxx");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
