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

## List

When set, `start_after` returns entries in lexicographic order and excludes
paths at or before the marker in both non-recursive and recursive listings.
Pass a path relative to the operator root, including the trailing `/` for a
directory entry. Pass an empty marker to start a sorted listing from the
beginning.

With `start_after`, the service reads and sorts each visited directory before
yielding its entries. This can increase latency and memory usage for large
directories. With `start_after` unset, the traversal follows the order of
entries from the filesystem.

## Delete

Deleting a symbolic link removes the link and preserves its target, even with
recursive deletion enabled. Use a path without a trailing `/` to delete a
dangling link.

A trailing `/` requires a directory target. A symbolic link to a file returns
[`opendal_core::ErrorKind::NotADirectory`] when addressed this way. If the link
target is missing, deletion through a directory path succeeds and leaves the
link in place.

## Configuration

Use [`crate::FsConfig`] for serializable configuration and this builder's
methods for direct construction. The field and method documentation defines
accepted values, defaults, and environment interaction.

### Direct I/O

Enable `direct_io` on Linux to bypass the kernel page cache for file reads and
writes. The service handles the alignment of buffers, offsets, and lengths.
Opening a file for direct I/O returns [`opendal_core::ErrorKind::Unsupported`]
if the backing filesystem or file does not support it. `copy` still uses
`tokio::fs::copy`. Append is unsupported when `direct_io` is enabled.

On Linux, finish all direct I/O before calling `fork()`. The service uses
heap-allocated buffers, and running `O_DIRECT` operations concurrently with
`fork()` can corrupt data.

## Example

### Via Builder

```rust,ignore
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Fs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create fs backend builder.
    let mut builder = Fs::default()
        // Set the root for fs, all operations will happen under this root.
        //
        // NOTE: the root must be absolute path.
        .root("/tmp");

    let op: Operator = Operator::new(builder)?;

    Ok(())
}
```
