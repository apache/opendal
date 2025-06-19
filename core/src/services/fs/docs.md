## Capabilities

This service can be used to:

- [x] stat
- [x] read
- [x] write
- [x] append
- [x] create_dir
- [x] delete
- [x] copy
- [x] rename
- [x] list
- [ ] ~~presign~~
- [x] blocking

## Configuration

- `root`: Set the work dir for backend.
- `atomic_write_dir`: Set temp dir for atomic write operations.
- `direct_io`: Enable direct IO mode to bypass page cache (requires `services-fs-direct-io` feature and Unix platform).

You can refer to [`FsBuilder`]'s docs for more information

### Direct IO Support

Direct IO mode bypasses the operating system's page cache, providing:

- Better control over memory usage
- Predictable I/O performance
- Useful for database management systems

**Requirements:**
- Must enable the `services-fs-direct-io` feature flag
- Only supported on Unix platforms (Linux, macOS, etc.)
- Requires proper memory alignment for optimal performance
- Not all filesystems support direct IO operations

## Example

### Via Builder


```rust,no_run
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

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```

### Direct IO Example

```rust,no_run
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Fs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create fs backend builder with direct IO enabled.
    let mut builder = Fs::default()
        .root("/tmp")
        // Enable direct IO mode (requires services-fs-direct-io feature)
        .direct_io(true);

    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
