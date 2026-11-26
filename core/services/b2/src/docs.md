## Capabilities

Depending on its configuration and the backing system, this service can expose:

- [ ] create_dir
- [x] stat
- [x] read
- [x] write
- [x] delete
- [x] list
- [x] copy
- [x] restore
- [ ] rename
- [x] presign

Inspect the effective capability set with [`opendal_core::Operator::info`] and
[`opendal_core::OperatorInfo::capability`] after building an operator.

## File metadata by version

`stat_with(path).version(file_id)` returns metadata for a B2 data version of
that file. The application key needs `readFiles` permission. A version that
has been permanently deleted, belongs to a different path, or is a hide marker
returns `NotFound`. Ordinary `stat` and `list` expose current file IDs through
`Metadata::version()`.

## File reads by version

`read_with(path).version(file_id)` and `reader_with(path).version(file_id)`
read a B2 data version of the specified file. The application key needs
`readFiles` permission. A version belonging to a different path returns
`NotFound`. The reader checks the version against the configured bucket and
path before each download. Range reads report the selected version's full length
in the reader's metadata. Response overrides with a selected version return
`Unsupported`.

## Presigned downloads

`presign_read` and `presign_stat` both return a B2 download request that uses
GET and addresses the file by name. `presign_read_options` and
`presign_stat_options` return `Unsupported` when a version is set. The
option-bearing variants support overriding the response
`Content-Type`, `Content-Disposition`, and `Cache-Control`. Use the returned
method, URL, and headers to execute the request.

## Restore

`restore(path)` removes one current B2 hide marker when a data version still
exists for the path. A live file keeps its current version, and a path with no
remaining data versions returns `NotFound`. The application key needs `listFiles`
and `deleteFiles` permissions to remove the marker.

`restore_with(path).version(file_id)` copies a data version of the same file to
make it current. Version promotion requires `readFiles` and `writeFiles`
and is subject to the [B2 Copy File size limit](https://www.backblaze.com/apidocs/b2-copy-file).

## Configuration

Use [`crate::B2Config`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

## Example

### Via Builder

```rust,no_run
use opendal_core::Operator;
use opendal_core::Result;
use opendal_service_b2::B2;

#[tokio::main]
async fn main() -> Result<()> {
    // create backend builder
    let mut builder = B2::default()
        // set the storage bucket for OpenDAL
        .root("/")
        // set the key_id for OpenDAL
        .application_key_id("xxxxxxxxxx")
        // set the key_id for OpenDAL
        .application_key("xxxxxxxxxx")
        // set the     bucket name for OpenDAL
        .bucket("opendal")
        // set the bucket_id for OpenDAL
        .bucket_id("xxxxxxxxxxxxx");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```
