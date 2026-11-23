> **Warning:** Verify conditional writes and deletes against your endpoint before
> relying on them. In the tested configurations, nginx’s `ngx_http_dav_module`
> ignored PUT preconditions and `If-Match` on DELETE. Apache `mod_dav_fs` allowed
> concurrent `If-None-Match: *` creates to succeed. Nextcloud and ownCloud could
> reuse an ETag after a same-second overwrite, allowing a stale `If-Match` to
> overwrite or delete newer content.
>
> Use [`CapabilityOverrideLayer`][opendal_core::layers::CapabilityOverrideLayer]
> to disable the affected capabilities. OpenDAL returns
> [`opendal_core::ErrorKind::Unsupported`] before sending a conditional write
> or delete to the server if the operation uses a disabled capability.

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

nginx’s `ngx_http_dav_module` omits ETags from DAV `PUT` responses.

## Configuration

Use [`crate::WebdavConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

### Recursive listing

Recursive listing sends a `PROPFIND` request with `Depth: infinity` by default.
Nextcloud and `dav-server` with infinity depth enabled return nested entries.
The tested nginx and ownCloud configurations reject infinity depth with HTTP
403 and 412, respectively. Jianguoyun has been reported to return only one
level. [Server compatibility reports](https://github.com/apache/opendal/issues/4256)

Set `disable_depth_infinity = true` for these endpoints. The default
`auto-simulate` feature installs `SimulateLayer`, which walks the directory
tree with `Depth: 1` requests for recursive listing. If you build without
`auto-simulate`, add `SimulateLayer::default()` to the operator to enable the
walk. With native recursion disabled and no simulation, recursive listing
returns `ErrorKind::Unsupported`.

```rust,no_run
use opendal_core::{Operator, Result};
use opendal_service_webdav::Webdav;

# fn main() -> Result<()> {
let op = Operator::new(
    Webdav::default()
        .endpoint("http://localhost:8080")
        .disable_depth_infinity(true),
)?;
# Ok(())
# }
```

### Recursive delete

Per [RFC 4918 §9.6.1](https://datatracker.ietf.org/doc/html/rfc4918#section-9.6.1),
a WebDAV `DELETE` on a collection always acts as `Depth: infinity`: the
server removes the collection and every member in one request, and clients
must not send `Depth: 0` or `Depth: 1`. OpenDAL sends one `DELETE` for
`delete("dir/")` and for `delete_with("dir/").recursive(true)`; there is no
client-side traversal. `disable_depth_infinity` only controls recursive
`PROPFIND` and does not affect this.

This means a non-recursive `delete("dir/")` also removes the directory's
contents, and `delete_with("/").recursive(true)` targets the configured root.

When the server cannot remove every member, it answers `207 Multi-Status`
with per-resource failures, for example on a locked or read-only subtree.
OpenDAL reports that response as an error while some descendants may already
have been deleted.

### Conditional writes and deletes

OpenDAL sends write preconditions on the PUT that writes the destination file
and delete preconditions on the DELETE that removes the target resource.
`if_match` on PUT and DELETE uses HTTP `If-Match` for strong ETags and a tagged
WebDAV `If` header (`If: <url> ([W/"..."])`) for weak ETags. `if_none_match` on
PUT uses HTTP `If-None-Match` for strong ETags, and sends both `If-None-Match`
and a tagged WebDAV `If` header (`If: <url> (Not [W/"..."])`) for weak ETags.
`if_not_exists` uses `If-None-Match: *`. OpenDAL reports HTTP 412 (and HTTP 404
on conditional DELETE) as [`opendal_core::ErrorKind::ConditionNotMatch`].

These conditional capabilities are enabled by default. Verify that your endpoint
honors the preconditions and preserves the destination on failure. These
results were observed using real servers and reading back the written files.
Each concurrent create trial used eight requests against the same missing path.

| Server | `if_match` (PUT & DELETE) | `if_none_match` | `if_not_exists` |
| --- | --- | --- | --- |
| nginx 1.30.7 with `ngx_http_dav_module` | Ignores both headers and overwrites or deletes the file. | Ignores the condition and overwrites the file. | Ignores the condition. All eight requests succeeded in each trial. |
| ownCloud 10.16.0 and Nextcloud 35.0.3 (SabreDAV) | Accepts a matching strong ETag and rejects a different one. Both reject the weak form and can reuse a strong ETag on same-second overwrites, allowing a stale match. | Rejects a matching strong ETag. Allows a different ETag or a missing file. | Rejects an existing file and creates a missing one. One request succeeded per trial. Other requests returned HTTP 412 or 423, or HTTP 404 on Nextcloud. |
| Apache HTTP Server 2.4.71 with `mod_dav_fs` | Accepts a matching strong or weak ETag on PUT and DELETE (including nested paths) and rejects a stale tag or missing target. | Rejects a matching strong or weak ETag (including a weak ETag after the server upgrades it to a strong ETag) and allows a different ETag or creation of a missing file. | Rejects an existing file sequentially. All eight requests succeeded in each concurrent trial. |
| `dav-server` 0.12.0 with `LocalFs` | Accepts a matching strong ETag and rejects a different one. A weak form of the current strong tag returned HTTP 412. | Rejects a matching strong ETag. Allows a different ETag or a missing file. | Rejects an existing file and creates a missing one. One request succeeded and seven returned HTTP 412 per trial. |

ownCloud, Nextcloud, and `dav-server` also accepted an overwrite when
`if_none_match` supplied the weak form of a current strong ETag.

For the nginx deployment above, use `CapabilityOverrideLayer` to disable all
four conditional write and delete capabilities.

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
        cap.delete_with_if_match = false;
        cap
    }));
assert!(!op.info().capability().write_with_if_not_exists);
# Ok(())
# }
```

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
