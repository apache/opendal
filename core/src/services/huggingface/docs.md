This service will visit the [Huggingface API](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api) to access the Huggingface File System.
Currently, we only support the `model` and `dataset` types of repositories, and operations are limited to reading and listing/stating.

Huggingface doesn't host official HTTP API docs. Detailed HTTP request API information can be found on the [`huggingface_hub` Source Code](https://github.com/huggingface/huggingface_hub).

## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [ ] write
- [ ] create_dir
- [ ] delete
- [ ] copy
- [ ] rename
- [x] list
- [ ] ~~presign~~
- [ ] blocking

## Configurations

- `repo_type`: The type of the repository.
- `repo_id`: The id of the repository.
- `revision`: The revision of the repository.
- `root`: Set the work directory for backend.
- `token`: The token for accessing the repository.

Refer to [`HuggingfaceBuilder`]'s public API docs for more information.

## Examples

### Via Builder

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Huggingface;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create Huggingface backend builder
    let mut builder = Huggingface::default()
        // set the type of Huggingface repository
        .repo_type("dataset")
        // set the id of Huggingface repository
        .repo_id("databricks/databricks-dolly-15k")
        // set the revision of Huggingface repository
        .revision("main")
        // set the root for Huggingface, all operations will happen under this root
        .root("/path/to/dir")
        // set the token for accessing the repository
        .token("access_token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
