# S3 Read on WASM

This test is used to ensure opendal can run on WASM target.

## Setup

Start the S3 server at `http://127.0.0.1:9900`

We are using the following config values, please feel free to change based on your own needs.

```rust
let mut cfg = S3::default();
cfg.endpoint("http://127.0.0.1:9900");
cfg.access_key_id("minioadmin");
cfg.secret_access_key("minioadmin");
cfg.bucket("opendal");
cfg.region("us-east-1");
```

## Install

```shell
cargo install wasm-pack
```

## Build

```shell
wasm-pack build
```

## Test

NOTE: 

- You need to have Chrome installed.
- We can't run on node.js yet.
- We can't run on headless chrome yet.

```shell
wasm-pack test --chrome
```
