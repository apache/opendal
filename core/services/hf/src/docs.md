This service will visit the [Hugging Face API](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api) to access the Hugging Face File System.

Hugging Face doesn't host official HTTP API docs. Detailed HTTP request API information can be found on the [`huggingface_hub` Source Code](https://github.com/huggingface/huggingface_hub).

Both `hf://` and `huggingface://` URI schemes are supported.

## Capabilities

This service can be used to:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [ ] delete
- [x] list
- [ ] copy
- [ ] rename
- [ ] ~~presign~~

## Configurations

- `repo_type`: The type of the repository (model, dataset, or space).
- `repo_id`: The id of the repository.
- `revision`: The revision of the repository.
- `root`: Set the work directory for backend.
- `token`: The token for accessing the repository. Required for write operations.

Refer to [`HfBuilder`]'s public API docs for more information.

## Examples

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_hf::Hf;

#[tokio::main]
async fn main() -> Result<()> {
    // Create Hugging Face backend builder
    let mut builder = Hf::default()
        // set the type of Hugging Face repository
        .repo_type("dataset")
        // set the id of Hugging Face repository
        .repo_id("databricks/databricks-dolly-15k")
        // set the revision of Hugging Face repository
        .revision("main")
        // set the root, all operations will happen under this root
        .root("/path/to/dir")
        // set the token for accessing the repository
        .token("access_token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
