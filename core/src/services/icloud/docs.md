## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [ ] write
- [ ] delete
- [ ] create_dir
- [ ] list
- [ ] copy
- [ ] rename
- [ ] batch


# Configuration

- `root`: Set the work directory for backend

### Credentials related

#### provide Session (Temporary)

- `trust_token`: set the trust_token for iCloud drive api
  Please notice its expiration.
- `ds_web_auth_token`: set the ds_web_auth_token for iCloud drive api
  Get web trust the session.
- `region`: set the region for iCloud drive api
  China region must use "https://www.icloud.com.cn"
  Otherwise Apple server will return 302.

OpenDAL is a library, it cannot do the first step of OAuth2 for you.
You need to get authorization code from user by calling iCloudDrive's authorize url
and save it for session's trust_token and session_token(ds_web_auth_token).

Make sure you have enabled Apple iCloud Drive API in your Apple iCloud ID.
And your OAuth scope contains valid `Session`.

You can get more information from [pyicloud](https://github.com/picklepete/pyicloud/tree/master?tab=readme-ov-file#authentication) or 
[iCloud-API](https://github.com/MauriceConrad/iCloud-API?tab=readme-ov-file#getting-started)

You can refer to [`IcloudBuilder`]'s docs for more information

## Example

### Via Builder

```rust
use anyhow::Result;
use opendal::services::Icloud;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Icloud::default();
    builder.root("/");
    builder.apple_id("<apple_id>");
    builder.password("<password>");
    builder.trust_token("<trust_token>");
    builder.ds_web_auth_token("<ds_web_auth_token>");
    builder.region("<region>");

    Ok(())
}