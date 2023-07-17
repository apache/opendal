# Fuzz Test for OpenDAL

fuzz test are used to test the robustness of the code. 

## Setup



To run the fuzz tests, please copy the `.env.example`, which is at project root, to `.env` and change the values on need.

Take `fs` for example, we need to change to enable behavior test on `fs` on `/tmp`.

```dotenv
OPENDAL_FS_TEST=false
OPENDAL_FS_ROOT=/path/to/dir
```

into

```dotenv
OPENDAL_FS_TEST=on
OPENDAL_FS_ROOT=/tmp
```


## Run

List all fuzz targets.

```bash
cargo +nightly fuzz list
```

Run a fuzz target(such as `reader`).

```bash
cargo +nightly fuzz run fuzz_reader
```

For more details, please visit [cargo fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/tutorial.html) or run the command cargo fuzz --help.