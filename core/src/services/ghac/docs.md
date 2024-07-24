## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [ ] rename
- [ ] list
- [ ] presign
- [ ] blocking

## Notes

This service is mainly provided by GitHub actions.

Refer to [Caching dependencies to speed up workflows](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows) for more information.

To make this service work as expected, please make sure to either call `endpoint` and `token` to
configure the URL and credentials, or that the following environment has been setup correctly:

- `ACTIONS_CACHE_URL`
- `ACTIONS_RUNTIME_TOKEN`

They can be exposed by following action:

```yaml
- name: Configure Cache Env
  uses: actions/github-script@v6
  with:
    script: |
      core.exportVariable('ACTIONS_CACHE_URL', process.env.ACTIONS_CACHE_URL || '');
      core.exportVariable('ACTIONS_RUNTIME_TOKEN', process.env.ACTIONS_RUNTIME_TOKEN || '');
```

To make `delete` work as expected, `GITHUB_TOKEN` should also be set via:

```yaml
env:
  GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

## Limitations

Unlike other services, ghac doesn't support create empty files.
We provide a `enable_create_simulation()` to support this operation but may result unexpected side effects.

Also, `ghac` is a cache service which means the data store inside could
be automatically evicted at any time.

## Configuration

- `root`: Set the work dir for backend.

Refer to [`GhacBuilder`]'s public API docs for more information.

## Example

### Via Builder

```no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Ghac;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create ghac backend builder.
    let mut builder = Ghac::default()
        // Set the root for ghac, all operations will happen under this root.
        //
        // NOTE: the root must be absolute path.
        .root("/path/to/dir");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
