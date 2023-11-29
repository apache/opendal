This service will visit the [HuggingFace API](https://huggingface.co/docs/huggingface_hub/package_reference/hf_api) to access the HuggingFace File System.
Currently, we only support the `model` and `dataset` types of repositories, and operations are limited to reading and listing/stating.

HuggingFace doesn't host official HTTP API docs. Detailed HTTP request API information can be found on the [HuggingFace Hub](https://github.com/huggingface/huggingface_hub).

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
- [ ] ~~scan~~
- [ ] ~~presign~~
- [ ] blocking

## Configurations

- `repo_type`: The type of the repository.
- `repo_id`: The id of the repository.
- `revision`: The revision of the repository.
- `root`: Set the work directory for backend.
- `token`: The token for accessing the repository.

Refer to [`Builder`]'s public API docs for more information.

## Examples

### Via Builder

```rust
use std::sync::Arc;

use anyhow::Result;
use opendal::services::HuggingFace;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create HuggingFace backend builder
    let mut builder = HuggingFace::default();
    
    // set the type of HuggingFace repository
    builder.repo_type("dataset");
    // set the id of HuggingFace repository
    builder.repo_id("databricks/databricks-dolly-15k");
    // set the revision of HuggingFace repository
    builder.revision("main");
    // set the root for HuggingFace, all operations will happen under this root
    builder.root("/path/to/dir");
    // set the token for accessing the repository
    builder.token("access_token");

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
