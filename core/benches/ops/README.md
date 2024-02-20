# Ops Benchmark Tests

Ops Benchmark Tests measure every operation's performance on the target platform.

To support benching different backends simultaneously, we use `environment value` to carry the backend config.

## Setup

Please copy `.env.example` to `.env` and change the values on need.

Take `fs` for example, we need to enable bench on `fs` on `/tmp`.

```dotenv
OPENDAL_FS_TEST=false
OPENDAL_FS_ROOT=/path/to/dir
```

into

```dotenv
OPENDAL_FS_TEST=on
OPENDAL_FS_ROOT=/tmp
```

Notice: The default will skip all benches if the env is not set.

## Run

Test all available backend.

```shell
cargo bench --features tests
```

Test specific backend, take s3 for example, first set the corresponding environment variables of s3, then:

```shell
OPENDAL_TEST=s3
cargo bench --features tests
```
