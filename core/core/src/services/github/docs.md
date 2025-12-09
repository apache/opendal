## Capabilities

This service can be used to:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [ ] rename
- [ ] presign

## Configuration

- `root`: Set the work directory for backend
- `token`: Github access token
- `owner`: Github owner
- `repo`: Github repository

You can refer to [`GithubBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal_core::services::Github;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Github::default()
        // set the storage root for OpenDAL
        .root("/")
        // set the access token for Github API
        .token("your_access_token")
        // set the owner for Github
        .owner("your_owner")
        // set the repository for Github
        .repo("your_repo");


    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
