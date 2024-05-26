## Capabilities

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [ ] list
- [ ] presign
- [ ] blocking

## Configuration

- `root`: Set the work dir for backend.
- `bucket`: Set the container name for backend.
- `endpoint`: Set the endpoint for backend.
- `key`: Set the authorization key for the backend, do not set if you want to read public bucket

### Authorization keys

There are two types of key in the Supabase, one is anon_key(Client key), another one is
service_role_key(Secret key). The former one can only write public resources while the latter one
can access all resources. Note that if you want to read public resources, do not set the key.

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Supabase;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Supabase::default();
    
    builder.root("/");
    builder.bucket("test_bucket");
    builder.endpoint("http://127.0.0.1:54321");
    // this sets up the anon_key, which means this operator can only write public resource
    builder.key("some_anon_key");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```

 
