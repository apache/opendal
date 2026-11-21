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
- [ ] ~~presign~~

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## Notes

Bazel Remote Caching and Ccache HTTP Storage is also part of this service.
Users can use `webdav` to connect those services.

`delete_with_if_match` is only as safe as the server's ETags. SabreDAV-based
servers such as Nextcloud and ownCloud derive ETags from one-second
modification times, so a stale ETag still matches after an overwrite within
the same second and the delete removes content the caller never observed.
Disable `delete_with_if_match` with `CapabilityOverrideLayer` for such servers.

nginx's `ngx_http_dav_module` omits ETags from DAV `PUT` responses and ignores
`If-Match` on `DELETE`, making deletes unconditional. Disable
`delete_with_if_match` with `CapabilityOverrideLayer` for these deployments as well.

## Configuration

Use [`crate::WebdavConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

### Streaming writes

WebDAV uses a single ordinary PUT by default. Enable `enable_partial_put`
with `WebdavBuilder::enable_partial_put(true)` or set the configuration field
to `true` to advertise `write_can_multi`. Empty and single-chunk uploads still
use one ordinary PUT. Conditional writes return `Unsupported`.

For a multi-chunk upload, OpenDAL creates a uniquely named temporary resource
in the destination directory and writes subsequent chunks with `Content-Range`.
Closing the writer publishes the completed resource with WebDAV MOVE and
`Overwrite: T`. The server determines whether that replacement is atomic.
Aborting deletes the temporary resource and preserves any existing destination.
OpenDAL also attempts to delete the temporary resource if publishing fails.

Enable this option only when the endpoint honors partial PUT with an unknown
complete length (`Content-Range: bytes start-end/*`) and supports MOVE with
overwrite. Observed server behavior:

| Server | Partial PUT with `Content-Range` |
| --- | --- |
| `dav-server` 0.12.0 with `LocalFs` | HTTP 204, preserves earlier bytes and supports MOVE. For an overwrite, this version deletes the old destination before moving the staged file. |
| Apache HTTP Server 2.4.71 with `mod_dav_fs` | HTTP 204, preserves earlier bytes and supports MOVE. |
| nginx 1.30.7 with `ngx_http_dav_module` | HTTP 501 Not Implemented. |
| ownCloud 10.16.0 and Nextcloud 35.0.3 (SabreDAV) | HTTP 400 with `Content-Range on PUT requests are forbidden.` |

## Example

### Via Builder

```rust,no_run
use anyhow::Result;
use opendal_service_webdav::Webdav;
use opendal_core::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = Webdav::default()
        .endpoint("127.0.0.1")
        .username("xxx")
        .password("xxx");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```
