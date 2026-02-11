This service provides access to Git repositories with transparent Git LFS support.

The service uses [gix](https://github.com/GitoxideLabs/gitoxide) for efficient Git operations and automatically resolves Git LFS pointer files by streaming the actual LFS content. This allows access to repositories hosted on GitHub, GitLab, Hugging Face, and custom Git servers.

## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [ ] write
- [ ] delete
- [x] list
- [ ] copy
- [ ] rename
- [ ] ~~presign~~

## Configuration

- `repository`: The Git repository URL (required)
- `reference`: The Git reference (branch, tag, or commit hash). If not set, uses whatever HEAD points to in the repository.
- `root`: Set the work directory for backend.
- `username`: Username for authentication (optional)
- `password`: Password or personal access token for authentication (optional)
- `resolve_lfs`: Whether to resolve Git LFS pointer files (default: true)

You can refer to [`GitBuilder`]'s docs for more information.

## Notes

- `resolve_lfs` defaults to `true` and returns LFS file contents instead of pointer files.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_git::Git;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Git::default()
        .repository("https://github.com/apache/opendal.git")
        .reference("main")
        .root("/core/src")
        .username("your-username")
        .password("your-token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
