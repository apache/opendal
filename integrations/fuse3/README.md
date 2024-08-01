# Apache OpenDAL™ fuse3 integration

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal/ci_integration_fuse3.yml?branch=main
[actions]: https://github.com/apache/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/fuse3_opendal.svg
[crates.io]: https://crates.io/crates/fuse3_opendal
[crate downloads]: https://img.shields.io/crates/d/fuse3_opendal.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

`fuse3_opendal` is an [`fuse3`](https://github.com/Sherlock-Holo/fuse3) implementation using opendal.

This crate can help you to access ANY storage services by mounting locally by [`FUSE`](https://www.kernel.org/doc/html/next/filesystems/fuse.html).

## Useful Links

- Documentation: [release](https://docs.rs/fuse3_opendal/) | [dev](https://opendal.apache.org/docs/fuse3-opendal/fuse3_opendal/)

## Examples

```rust
use fuse3::path::Session;
use fuse3::MountOptions;
use fuse3::Result;
use fuse3_opendal::Filesystem;
use opendal::services::Memory;
use opendal::Operator;

#[tokio::test]
async fn test() -> Result<()> {
    // Build opendal Operator.
    let op = Operator::new(Memory::default())?.finish();

    // Build fuse3 file system.
    let fs = Filesystem::new(op, 1000, 1000);

    // Configure mount options.
    let mount_options = MountOptions::default();

    // Start a fuse3 session and mount it.
    let mut mount_handle = Session::new(mount_options)
        .mount_with_unprivileged(fs, "/tmp/mount_test")
        .await?;
    let handle = &mut mount_handle;

    tokio::select! {
        res = handle => res?,
        _ = tokio::signal::ctrl_c() => {
            mount_handle.unmount().await?
        }
    }

    Ok(())
}
```

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
