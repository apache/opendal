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

## Notes

This service targets SharePoint document libraries through the Microsoft Graph
`driveItem` API. For a personal OneDrive, use `services-onedrive` instead.

### Anchoring

An operator is anchored to the folder named by `folder_url`. The URL is resolved
once, on first use, into a drive id and an item id through Graph's
[`/shares`](https://learn.microsoft.com/en-us/graph/api/shares-get) endpoint;
files below it are then addressed by path relative to that item.

Because the resolved item id is immutable, renaming the site, the document
library, or the anchor folder itself does not invalidate a configured operator.
Renaming a *file* does change its path, as it would on any filesystem.

### Naming restrictions

SharePoint enforces stricter naming rules than a personal OneDrive, and rejects
requests that violate them. OpenDAL passes paths through unchanged and surfaces
the rejection, so callers are responsible for staying within these limits:

- Reserved characters: `/ \ * < > ? : |`, plus `#` and `%`. Support for `#` and
  `%` can be enabled by a tenant administrator, which is why this service does
  not reject them client-side.
- Names cannot begin with `~`, end with `.`, contain consecutive `..`, or have
  leading or trailing spaces.
- Reserved names: `.lock`, `CON`, `PRN`, `AUX`, `NUL`, `COM0`-`COM9`,
  `LPT0`-`LPT9`, `_vti_`, `desktop.ini`, and anything starting with `~$`.
- The total path length cannot exceed 400 characters.

Read more at
[Restrictions and limitations in OneDrive and SharePoint](https://support.microsoft.com/en-us/office/restrictions-and-limitations-in-onedrive-and-sharepoint-64883a5d-228e-48f5-b3d2-eb39e07630fa).

Percent-encoding of otherwise legal names is handled by this service.

### Library configuration can block writes

These are properties of the target library rather than defects, and surface as
errors from the service:

- A library with required metadata columns can reject uploads.
- A library that requires check-out can leave files locked.
- Retention labels or legal holds can make delete fail.

### Consistency issues with concurrent requests

SharePoint does not guarantee consistency when handling a large number of
concurrent write operations.

In some extreme cases, it may acknowledge an operation as successful but fail to
commit the changes. This inconsistency can cause subsequent operations to fail,
returning errors like:

- 400 Bad Request: folders in the path are considered not there yet
- 404 Not Found: the created folder is not recognized
- 409 Conflict: an existing folder cannot be replaced

Consider using
[`RetryLayer`](https://docs.rs/opendal-layer-retry/latest/opendal_layer_retry/struct.RetryLayer.html)
and monitor operations carefully.

## Configuration

Use [`crate::SharepointConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

`folder_url` is required. The configuration for tokens is one of the following:

- `access_token` only, for short-lived access. Once the `access_token` expires, you must recreate the operator with a new token.
- `refresh_token`, `client_id`, and an optional `client_secret`, for long-lived access. The operator will automatically get and refresh the access token.

Set `tenant_id` unless the application is registered as multi-tenant; it defaults
to `common`.

## How to get tokens

1. Create an application: navigate to [Microsoft Entra Admin Center](https://entra.microsoft.com/) -> Applications -> App Registrations -> New registration
2. In "Supported account types", choose the option matching your tenant. Unlike a personal OneDrive, SharePoint sites belong to a work or school tenant, so set `tenant_id` to that tenant's ID or domain name.
3. The platform you choose determines whether you have to provide a `client_secret` or not. See [Public and confidential client apps](https://learn.microsoft.com/en-us/entra/identity-platform/msal-client-applications) for more information.
   1. In short, if you choose "Mobile and desktop applications" or "Single-page application" (Public Client), you must not provide `client_secret`.
   2. If you choose "Web" (Confidential Client), create a secret in "Certificates & secrets -> Client secrets -> New client secret", and provide it as `client_secret`.
4. Follow the [code grant flow](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow) or other flows to get the access_token. The minimum scope is `Sites.ReadWrite.All`. The access token must represent a user, so the "client credentials flow" is not supported by this service.
5. If you need `refresh_token` for long-lived access, add an additional `offline_access` scope.

To find `folder_url`, open the target folder in SharePoint and copy the URL from
the browser address bar.

## Example

### Via Builder

When you have a current access token:

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_sharepoint::Sharepoint;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let builder = Sharepoint::default()
        .folder_url("https://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports")
        .access_token("my_access_token");

    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```

When you have an Application with a refresh token:

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_sharepoint::Sharepoint;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let builder = Sharepoint::default()
        .folder_url("https://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports")
        .tenant_id("my_tenant_id")
        .refresh_token("my_refresh_token")
        .client_id("my_client_id")
        .root("/subfolder/for/operator");

    let op: Operator = Operator::new(builder)?;
    Ok(())
}
```

[conflict-behavior]: https://learn.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0#instance-attributes
