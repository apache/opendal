## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [ ] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [x] list
- [ ] presign
- [ ] blocking

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
use opendal::services::Github;
use opendal::Operator;

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
