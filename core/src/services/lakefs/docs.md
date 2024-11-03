This service will visit the [Lakefs API](https://Lakefs.co/docs/Lakefs_hub/package_reference/hf_api) to access the Lakefs File System.
Currently, we only support the `model` and `dataset` types of repositories, and operations are limited to reading and listing/stating.

Lakefs doesn't host official HTTP API docs. Detailed HTTP request API information can be found on the [`Lakefs_hub` Source Code](https://github.com/Lakefs/Lakefs_hub).

## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [ ] create_dir
- [x] delete
- [x] copy
- [ ] rename
- [x] list
- [ ] ~~presign~~
- [ ] blocking

## Configurations

- `endpoint`: The endpoint of the Lakefs repository.
- `repository`: The id of the repository.
- `branch`: The branch of the repository.
- `root`: Set the work directory for backend.
- `username`: The username for accessing the repository.
- `password`: The password for accessing the repository.

Refer to [`LakefsBuilder`]'s public API docs for more information.

## Examples

### Via Builder

```rust,no_run
use opendal::Operator;
use opendal::services::Lakefs;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Create Lakefs backend builder
    let mut builder = Lakefs::default()
        // set the type of Lakefs endpoint
        .endpoint("https://whole-llama-mh6mux.us-east-1.lakefscloud.io")
        // set the id of Lakefs repository
        .repository("sample-repo")
        // set the branch of Lakefs repository
        .branch("main")
        // set the username for accessing the repository
        .username("xxx")
        // set the password for accessing the repository
        .password("xxx");

    let op: Operator = Operator::new(builder)?.finish();

    let stat = op.stat("README.md").await?;
    println!("{:?}", stat);
    Ok(())
}
```
