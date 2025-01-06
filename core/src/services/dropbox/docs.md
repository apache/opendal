## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [x] batch
- [ ] blocking

## Configuration

- `root`: Set the work directory for this backend.

### Credentials related

#### Just provide Access Token (Temporary)

- `access_token`: set the access_token for this backend.
Please notice its expiration.

#### Or provide Client ID and Client Secret and refresh token (Long Term)

If you want to let OpenDAL to refresh the access token automatically,
please provide the following fields:

- `refresh_token`: set the refresh_token for dropbox api
- `client_id`: set the client_id for dropbox api
- `client_secret`: set the client_secret for dropbox api

OpenDAL is a library, it cannot do the first step of OAuth2 for you.
You need to get authorization code from user by calling Dropbox's authorize url
and exchange it for refresh token.

Please refer to [Dropbox OAuth2 Guide](https://www.dropbox.com/developers/reference/oauth-guide)
for more information.

You can refer to [`DropboxBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::raw::OpWrite;
use opendal::services::Dropbox;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Dropbox::default()
        .root("/opendal")
        .access_token("<token>");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```
