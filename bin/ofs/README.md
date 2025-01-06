# Apache OpenDAL™ ofs

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal/ci_bin_ofs.yml?branch=main
[actions]: https://github.com/apache/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/ofs.svg
[crates.io]: https://crates.io/crates/ofs
[crate downloads]: https://img.shields.io/crates/d/ofs.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

`ofs` is a userspace filesystem backing by OpenDAL.

## Status

`ofs` is a work in progress. we only support `fs` and `s3` as backend on `Linux` currently.

## How to use `ofs`

### Install `FUSE` on Linux

```shell
sudo pacman -S fuse3 --noconfirm # archlinux
sudo apt-get -y install fuse     # debian/ubuntu
```

### Load `FUSE` kernel module on FreeBSD

```shell
kldload fuse
```

### Install `ofs`

`ofs` could be installed by `cargo`:

```shell
cargo install ofs
```

> `cargo` is the Rust package manager. `cargo` could be installed by following the [Installation](https://www.rust-lang.org/tools/install) from Rust official website.

### Mount directory

```shell
ofs <mount-point> 'fs://?root=<directory>'
```

### Mount S3 bucket

```shell
ofs <mount-point> 's3://?root=<path>&bucket=<bucket>&endpoint=<endpoint>&region=<region>&access_key_id=<access-key-id>&secret_access_key=<secret-access-key>'
```

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
