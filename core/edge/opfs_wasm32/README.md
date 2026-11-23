# OPFS on WASM

This test verifies the OpenDAL OPFS service works in a browser environment.

## Install

```shell
cargo install wasm-pack
```

## Build

```shell
wasm-pack build
```

## Test

Run these commands from this directory with Chrome and a matching ChromeDriver.
The Edge Test workflow runs both suites in headless Chrome.

### Main thread

```shell
wasm-pack test --headless --chrome -- --locked
```

### Dedicated worker

```shell
wasm-pack test --headless --chrome -- --locked --features worker
```
