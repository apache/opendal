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

- `trust_token`: set the trust_token for icloud drive api
  Please notice its expiration.
- `ds_web_auth_token`: set the ds_web_auth_token for icloud drive api
  Get web trust the session.
- `is_china_mainland`: set the is_china_mainland for icloud drive api
  China region must true to use <https://www.icloud.com.cn>
  Otherwise Apple server will return 302.
  More information you can get [apple.com](https://support.apple.com/en-us/111754)

OpenDAL is a library, it cannot do the first step of OAuth2 for you.
You need to get authorization code from user by calling icloudDrive's authorize url
and save it for session's trust_token and session_token(ds_web_auth_token).

Make sure you have enabled Apple icloud Drive API in your Apple icloud ID.
And your OAuth scope contains valid `Session`.

You can get more information from [pyicloud](https://github.com/picklepete/pyicloud/tree/master?tab=readme-ov-file#authentication) or 
[iCloud-API](https://github.com/MauriceConrad/iCloud-API?tab=readme-ov-file#getting-started)

You can refer to [`IcloudBuilder`]'s docs for more information

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal::services::Icloud;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    let mut builder = Icloud::default()
      .root("/")
      .apple_id("<apple_id>")
      .password("<password>")
      .trust_token("<trust_token>")
      .ds_web_auth_token("<ds_web_auth_token>")
      .is_china_mainland(true);

    Ok(())
}
