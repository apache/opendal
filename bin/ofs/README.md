# OpenDAL File System

OpenDAL File System (ofs) is a userspace filesystem backing by OpenDAL.

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

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
