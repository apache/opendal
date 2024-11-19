# Behavior Test for OpenDAL

Behavior tests are used to make sure every service works correctly.

To support different testing backends simultaneously, we use `environment value` to carry the backend config.

## Setup

To run the behavior tests, please copy the `.env.example`, which is at project root, to `.env` and change the values on need.

Take `fs` for example, we need to change to enable behavior test on `fs` on `/tmp`.

```dotenv
OPENDAL_FS_ROOT=/path/to/dir
```

into

```dotenv
OPENDAL_FS_ROOT=/tmp
```

Notice: If the env variables are not set, all behavior tests will be skipped by default.

## Run

Use `OPENDAL_TEST` to control which service to test:

```shell
OPENDAL_TEST=fs cargo test behavior --features tests
```

To run certain types of tests(such as `write`), we use `behavior::test_write`.

```shell
OPENDAL_TEST=fs cargo test behavior::test_write --features tests
```

You can also run specific test(such as `test_stat_dir`) for specific backend.

```shell
OPENDAL_TEST=fs cargo test behavior::test_stat_dir --features tests
```

## Debug

To debug a behavior test, you can:

- Add env `RUST_LOG=debug` to enable logging
- Add env `RUST_BACKTRACE=full` to enable the full backtrace
- Add `--show-output` to show the whole output even when test succeeded.

Take `memory` service as an example, the full command will be:

```shell
RUST_LOG=debug RUST_BACKTRACE=full OPENDAL_TEST=memory cargo test behavior --features tests -- --show-output
```

You use `export` to avoid set env every time:

```shell
export RUST_LOG=debug 
export RUST_BACKTRACE=full 
OPENDAL_TEST=memory cargo test behavior --features tests -- --show-output
```

For more details, please visit [cargo test](https://doc.rust-lang.org/cargo/commands/cargo-test.html) or run the command `cargo test --help`.
