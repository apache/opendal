This service provides access to Git repositories with transparent Git LFS support.

The service uses [gix](https://github.com/GitoxideLabs/gitoxide) for efficient Git operations and automatically handles Git LFS pointer files by streaming the actual LFS content. This allows seamless access to Git repositories including those hosted on GitHub, GitLab, Hugging Face, and custom Git servers.

## Key Features

- **Transparent LFS Handling**: Automatically detects and resolves Git LFS pointer files
- **Streaming I/O**: Efficient streaming of file contents without full repository clones
- **Multiple Providers**: Supports GitHub, GitLab, Hugging Face, and custom Git servers
- **Metadata Access**: Use gix for repository metadata and tree traversal
- **Smart Protocol**: Uses Git's smart HTTP protocol for efficient data transfer

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

## Configurations

- `repository`: The Git repository URL (required)
- `reference`: The Git reference (branch, tag, or commit hash). If not set, uses whatever HEAD points to in the repository.
- `root`: Set the work directory for backend
- `username`: Username for authentication (optional)
- `password`: Password or personal access token for authentication (optional)
- `resolve_lfs`: Whether to resolve Git LFS pointer files (default: true)

Refer to [`GitBuilder`]'s public API docs for more information.

## Testing

Configure `.env` with credentials and run behavior tests:

```bash
# In .env
OPENDAL_GIT_REPOSITORY=https://huggingface.co/Qwen/Qwen3-0.6B
OPENDAL_GIT_USERNAME=your_username
OPENDAL_GIT_PASSWORD=your_token
OPENDAL_GIT_RESOLVE_LFS=true

# Run tests
OPENDAL_TEST=git cargo test behavior --features tests,services-git
```

## Examples

### Via Builder

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Git;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create Git backend builder
    let mut builder = Git::default()
        // Set the repository URL
        .repository("https://github.com/apache/opendal.git")
        // Optionally set a specific git reference (branch, tag, or commit)
        // If not set, will use the repository's default branch (HEAD)
        .reference("main")
        // Set the root path within the repository
        .root("/core/src")
        // Set authentication credentials (optional)
        .username("your-username")
        .password("your-token");

    let op: Operator = Operator::new(builder)?.finish();

    // Read a file - LFS files are automatically resolved
    let content = op.read("README.md").await?;
    
    Ok(())
}
```

### Accessing Hugging Face Models

**Note:** For HuggingFace repositories, we recommend using the dedicated [`Huggingface`](https://docs.rs/opendal/latest/opendal/services/struct.Huggingface.html) service,
which is optimized for HuggingFace-specific features. This example demonstrates Git service compatibility for completeness.

```rust,no_run
use anyhow::Result;
use opendal::services::Git;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Access HuggingFace model repository via Git protocol
    let builder = Git::default()
        .repository("https://huggingface.co/meta-llama/Llama-2-7b")
        .resolve_lfs(true); // Enable LFS for model files

    let op: Operator = Operator::new(builder)?.finish();
    
    // Transparently access large model files (LFS)
    let model_data = op.read("pytorch_model.bin").await?;
    
    Ok(())
}
```

### Listing Repository Contents

```rust,no_run
use anyhow::Result;
use futures::TryStreamExt;
use opendal::services::Git;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Uses repository's default branch (HEAD)
    let builder = Git::default()
        .repository("https://github.com/apache/opendal.git")
        .root("/");

    let op: Operator = Operator::new(builder)?.finish();
    
    // List files in the repository
    let mut lister = op.lister("/").await?;
    while let Some(entry) = lister.try_next().await? {
        println!("{}: {} bytes", entry.path(), entry.metadata().content_length());
    }
    
    Ok(())
}
```

