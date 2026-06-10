This service will visit the [Hugging Face API](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api) to access the Hugging Face File System.

Hugging Face doesn't host official HTTP API docs. Detailed HTTP request API information can be found on the [`huggingface_hub` Source Code](https://github.com/huggingface/huggingface_hub).

## Storage Backends

This service supports two storage backends:

- **Git-based repositories** (`model`, `dataset`, `space`): Files are versioned in a Git repository. Large files are stored via [Xet](https://huggingface.co/docs/hub/xet/index), Hugging Face's chunk-deduplicated storage backend; writes create new commits. Supports `revision` for branch/commit targeting.
- **Object store buckets** (`bucket`): Files are stored in a Hugging Face Bucket (not git-backed). No revisions or commits — all reads and writes use the [Xet](https://huggingface.co/docs/hub/xet/index) protocol directly.

## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [ ] copy
- [ ] rename
- [ ] presign

## Configurations

- `repo_type`: The type of the repository. One of `model`, `dataset`, `space`, or `bucket`.
- `repo_id`: The id of the repository.
- `revision`: The revision of the repository. Only applicable for git-based repo types (`model`, `dataset`, `space`).
- `root`: Set the work directory for backend.
- `token`: The token for accessing the repository. Required for write operations.
- `endpoint`: The Hub base URL. Default is `https://huggingface.co`. Can also be set via the `HF_ENDPOINT` environment variable.

Refer to [`HfBuilder`]'s public API docs for more information.

## Examples

### Via Builder (Git-based dataset)

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_hf::Hf;

#[tokio::main]
async fn main() -> Result<()> {
    let builder = Hf::default()
        .repo_type("dataset")
        .repo_id("username/my-dataset")
        .revision("main")
        .root("/path/to/dir")
        .token("access_token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```

### Via Builder (Object store bucket)

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_hf::Hf;

#[tokio::main]
async fn main() -> Result<()> {
    let builder = Hf::default()
        .repo_type("bucket")
        .repo_id("username/my-bucket")
        .token("access_token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```

### Via URI

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Git-based dataset
    let op = Operator::from_uri((
        "hf://datasets/username/my-dataset@main",
        vec![("token", "access_token")],
    ))?;

    // Object store bucket
    let op = Operator::from_uri((
        "hf://buckets/username/my-bucket",
        vec![("token", "access_token")],
    ))?;

    Ok(())
}
```
