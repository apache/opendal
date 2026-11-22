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

### Conditional writes

OpenDAL sends write preconditions on the PUT that writes the destination file.
`if_match` uses HTTP `If-Match` for strong ETags and WebDAV `If` for weak ETags.
`if_none_match` uses HTTP `If-None-Match`, and `if_not_exists` uses
`If-None-Match: *`. OpenDAL reports HTTP 412 as
[`opendal_core::ErrorKind::ConditionNotMatch`].

The three write capabilities are enabled by default. Verify that your endpoint
honors the preconditions and preserves the destination on failure. These
results were observed using real servers and reading back the written files.
Each concurrent create trial used eight requests against the same missing path.

| Server | `if_match` | `if_none_match` | `if_not_exists` |
| --- | --- | --- | --- |
| nginx 1.30.7 with `ngx_http_dav_module` | Ignores both headers and overwrites the file. | Ignores the condition and overwrites the file. | Ignores the condition. All eight requests succeeded in each trial. |
| ownCloud 10.16.0 and Nextcloud 35.0.3 (SabreDAV) | Accepts a matching strong ETag and rejects a different one. Both reject the weak form and can reuse a strong ETag on same-second overwrites, allowing a stale match. | Rejects a matching strong ETag. Allows a different ETag or a missing file. | Rejects an existing file and creates a missing one. One request succeeded per trial. Other requests returned HTTP 412 or 423, or HTTP 404 on Nextcloud. |
| Apache HTTP Server 2.4.71 with `mod_dav_fs` | Accepts a matching strong or weak ETag and rejects a stale tag. | Rejects a matching strong ETag and allows creation of a missing file. A matching weak ETag immediately after a write allowed an overwrite. | Rejects an existing file sequentially. All eight requests succeeded in each concurrent trial. |
| `dav-server` 0.12.0 with `LocalFs` | Accepts a matching strong ETag and rejects a different one. A weak form of the current strong tag returned HTTP 412. | Rejects a matching strong ETag. Allows a different ETag or a missing file. | Rejects an existing file and creates a missing one. One request succeeded and seven returned HTTP 412 per trial. |

Apache HTTP Server, ownCloud, Nextcloud, and `dav-server` also accepted an
overwrite when `If-None-Match` supplied the weak form of a current strong ETag.

Use [`opendal_core::layers::CapabilityOverrideLayer`] to report the affected
capabilities as disabled for endpoints that cannot enforce them. Check the
operator's effective capabilities before using a write condition. For example,
report all three as disabled for the nginx deployment above.

```rust,no_run
use opendal_core::layers::CapabilityOverrideLayer;
use opendal_core::{Operator, Result};
use opendal_service_webdav::Webdav;

# fn main() -> Result<()> {
let op = Operator::new(Webdav::default().endpoint("http://localhost:8080"))?
    .layer(CapabilityOverrideLayer::new(|mut cap| {
        cap.write_with_if_match = false;
        cap.write_with_if_none_match = false;
        cap.write_with_if_not_exists = false;
        cap
    }));
# Ok(())
# }
```

### Streaming writes

WebDAV uses a single ordinary PUT by default. Enable `enable_partial_put`
with `Webdav::enable_partial_put(true)` or set the configuration field
to `true` to advertise `write_can_multi`. Empty and single-chunk uploads still
use one ordinary PUT. Conditional writes always use a single PUT, even when
partial PUT is enabled. Writing multiple chunks with a condition returns
`Unsupported`.

For a multi-chunk upload, OpenDAL creates a uniquely named temporary resource
in the destination directory and writes subsequent chunks with `Content-Range`.
Closing the writer publishes the completed resource with WebDAV MOVE and
`Overwrite: T`. The backing server controls whether the overwrite is atomic.
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
