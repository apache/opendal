## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [ ] write (WIP)
- [ ] create_dir
- [x] delete
- [ ] copy
- [ ] rename
- [x] list
- [ ] scan
- [ ] presign
- [ ] blocking

## Notes

This service is mainly provided by GitHub actions.

Refer to [Storing workflow data as artifacts](https://docs.github.com/en/actions/using-workflows/storing-workflow-data-as-artifacts) for more information.


## Configuration

- `owner`: Set the account owner of the repository. The name is not case sensitive.
- `repo`: Set the name of the repository without the .git extension. The name is not case sensitive.
- `token`: Set token of this backend
  - If the repository is private you must use an access token with the repo scope.
   - You must authenticate using an access token with the repo scope to delete, read artifacts.

Refer to [`GhaaBuilder`]'s public API docs for more information.

## Example

### Via Builder

```no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Ghaa;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create ghaa backend builder.
    let mut builder = Ghaa::default();
    
    // Set the owner
    builder.owner("foo");
    // Set the repo
    builder.repo("bar");
    // Set the token
    builder.token("github_pat_xxxxxxxxx");
    
    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
