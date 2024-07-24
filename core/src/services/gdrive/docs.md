## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] create_dir
- [x] list
- [x] copy
- [x] rename
- [ ] batch


# Configuration

- `root`: Set the work directory for backend

### Credentials related

#### Just provide Access Token (Temporary)

- `access_token`: set the access_token for google drive api
Please notice its expiration.

#### Or provide Client ID and Client Secret and refresh token (Long Term)

If you want to let OpenDAL to refresh the access token automatically,
please provide the following fields:

- `refresh_token`: set the refresh_token for google drive api
- `client_id`: set the client_id for google drive api
- `client_secret`: set the client_secret for google drive api

OpenDAL is a library, it cannot do the first step of OAuth2 for you.
You need to get authorization code from user by calling GoogleDrive's authorize url
and exchange it for refresh token.

Make sure you have enabled Google Drive API in your Google Cloud Console.
And your OAuth scope contains `https://www.googleapis.com/auth/drive`.

Please refer to [GoogleDrive OAuth2 Flow](https://developers.google.com/identity/protocols/oauth2/)
for more information.

You can refer to [`GdriveBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Gdrive;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Gdrive::default()
        .root("/test")
        .access_token("<token>");

    Ok(())
}

