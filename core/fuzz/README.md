# Fuzz Test for OpenDAL

fuzz test are used to test the robustness of the code. 

## Setup

To run the fuzz tests, you need to switch your rust version to nightly. You can do this by running the following command:

```bash
rustup override set nightly
```

## Run

List all fuzz targets.

```bash
cargo fuzz list
```

Run a fuzz target(such as `fs` `reader`).

```bash
cargo fuzz run fuzz_reader_fs
```

For more details, please visit [cargo fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/tutorial.html) or run the command cargo fuzz --help.