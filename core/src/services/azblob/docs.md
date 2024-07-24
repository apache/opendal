## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] append
- [x] create_dir
- [x] delete
- [x] copy
- [ ] rename
- [x] list
- [x] presign
- [ ] blocking

## Configuration

- `root`: Set the work dir for backend.
- `container`: Set the container name for backend.
- `endpoint`: Set the endpoint for backend.
- `account_name`: Set the account_name for backend.
- `account_key`: Set the account_key for backend.

Refer to public API docs for more information.

## Examples

This example works on [Azurite](https://github.com/Azure/Azurite) for local developments.

### Start local blob service

```shell
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
az storage container create --name test --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
```

### Init OpenDAL Operator

### Via Builder

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Azblob;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create azblob backend builder.
    let mut builder = Azblob::default()
        // Set the root for azblob, all operations will happen under this root.
        //
        // NOTE: the root must be absolute path.
        .root("/path/to/dir")
        // Set the container name, this is required.
        .container("test")
        // Set the endpoint, this is required.
        //
        // For examples:
        // - "http://127.0.0.1:10000/devstoreaccount1"
        // - "https://accountname.blob.core.windows.net"
        .endpoint("http://127.0.0.1:10000/devstoreaccount1")
        // Set the account_name and account_key.
        //
        // OpenDAL will try load credential from the env.
        // If credential not set and no valid credential in env, OpenDAL will
        // send request without signing like anonymous user.
        .account_name("devstoreaccount1")
        .account_key("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
