# Behavior Test

Behavior Tests are used to make sure every service works correctly.

To support different testing backends simultaneously, we use `environment value` to carry the backend config.

## Setup

To run the behavior test, please copy `.env.example` to `.env` and change the values on need.

Take `fs` for example. We need to change to enable behavior test on `fs` on `/tmp`.

```dotenv
OPENDAL_FS_TEST=false
OPENDAL_FS_ROOT=/path/to/dir
```

into

```dotenv
OPENDAL_FS_TEST=on
OPENDAL_FS_ROOT=/tmp
```

Notice: If the env is not set, all behavior tests will be skipped by default.

## Run

Test all available backend.

```shell
cargo test
```

Test specific backend.

```shell
cargo test fs
```
