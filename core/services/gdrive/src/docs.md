## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [x] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [x] rename
- [ ] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Behavior Notes

Google Drive allows duplicate file or directory names under the same parent.
When multiple entries match the same path, OpenDAL resolves the most recently
modified match, falling back to the newer creation time if needed.

## Configuration

Use [`crate::GdriveConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

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

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_gdrive::Gdrive;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Gdrive::default()
        .root("/test")
        .access_token("<token>");

    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```
