# Use fs as backend

[fs.rs](fs.rs) provides a detailed examples for using fs as backend.

```shell
cargo run --example fs
```

All config could be passed via environment:

- `OPENDAL_FS_ROOT`: root path, default: `/tmp`
