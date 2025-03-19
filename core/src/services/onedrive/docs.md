## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [ ] append
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [ ] presign
- [ ] blocking

## Notes

Currently, OpenDAL supports OneDrive Personal only.

### Write Operations and OneDrive Behavior

For write-related operations, such as:

- write
- rename
- copy
- create_dir

OpenDAL's OneDrive service replaces the destination folder instead of rename it.

### Consistency Issues with Concurrent Requests

OneDrive does not guarantee consistency when handling a large number of concurrent requests write operations.

In some extreme cases, OneDrive may acknowledge an operation as successful but fail to commit the changes.
This inconsistency can cause subsequent operations to fail, returning errors like:

- 400 Bad Request: OneDrive considers folders in the path are not there yet
- 404 Not Found: OneDrive doesn't recognize the created folder
- 409 Conflict: OneDrive can't replace an existing folder

You should consider [`RetryLayer`] and monitor your operations carefully.

## Configuration

- `access_token`: Set a short-live access token for Microsoft Graph API (also, OneDrive API)
- `refresh_token`: Set a long term access token for Microsoft Graph API
- `client_id`: Set the client ID for a Microsoft Graph API application (available though Azure's registration portal)
- `client_secret`: Set the client secret for a Microsoft Graph API application
- `root`: Set the work directory for OneDrive backend
- `enable_versioning`: Enable versioning support for OneDrive items

The configuration for tokens is one of the following:
* `access_token` only, for short-lived access. Once the `access_token` expires, you must recreate the operator with a new token.
* `refresh_token`, `client_id`, and an optional `client_secret`, for long-lived access. The operator will automatically get and refresh the access token.

## How to get tokens

1. Create an application: navigate to [Microsoft Entra Admin Center](https://entra.microsoft.com/) -> Applications -> App Registrations -> New registration
2. In "Supported account types", choose "Accounts in any organizational directory (Any Microsoft Entra ID tenant - Multitenant) and personal Microsoft accounts (e.g. Skype, Xbox)"
3. Or in an existing application -> manifest, make sure `signInAudience` is `"AzureADandPersonalMicrosoftAccount"`. That's because we're calling Graph API with `/common` path segment.
4. The platform you choose determines whether you have to provide a `client_secret` or not. See [Public and confidential client apps](https://learn.microsoft.com/en-us/entra/identity-platform/msal-client-applications) for more information.
    1. In short, if you choose "Mobile and desktop applications" or "Single-page application" (Public Client), you must not provide `client_secret`.
    2. If you choose "Web" (Confidential Client), create a secret in "Certificates & secrets -> Client secrets -> New client secret", and provide it as `client_secret`.
5. Follow the [code grant flow](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow) or other flows to get the access_token. The minimum scope is `Files.ReadWrite`. And make sure the access token represents a user, because it's accessing the user's onedrive by `/me/drive`. So "client credentials flow" won't work.
6. If you need `refresh_token` for long-lived access, add an additional `offline_access` scope.

Read more at [`OnedriveBuilder`].

## Example

### Via Builder

When you have a current access token: 

```rust,no_run
use anyhow::Result;
use opendal::services::Onedrive;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Onedrive::default()
        .access_token("my_access_token")
        .root("/root/folder/for/operator");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```

When you have an Application with a refresh token:

```rust,no_run
use anyhow::Result;
use opendal::services::Onedrive;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Onedrive::default()
        .refresh_token("my_refresh_token")
        .client_id("my_client_id")
        .root("/root/folder/for/operator");

    let op: Operator = Operator::new(builder)?.finish();
    Ok(())
}
```


[conflict-behavior]: https://learn.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0#instance-attributes
