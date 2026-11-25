# S3 Read on WASM

This edge test verifies that OpenDAL can write an S3 object on
`wasm32-unknown-unknown` and read back the same payload in Node.js and Chrome.

## Setup

Install [wasm-pack](https://wasm-bindgen.github.io/wasm-pack/installer/) and the
WASM target. Use Node.js 20 or later for its built-in `fetch` API. For the browser
test, install Chrome and put a matching ChromeDriver on your `PATH`.

```sh
rustup target add wasm32-unknown-unknown
```

From the repository root, start the pinned Silo S3 fixture used in CI and
create the `test` bucket with the AWS CLI.

```sh
docker compose -f fixtures/s3/docker-compose-silo.yml up -d --wait
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_EC2_METADATA_DISABLED=true \
  aws --region us-east-1 --endpoint-url http://127.0.0.1:9000 s3 mb s3://test
```

You can also use a local MinIO server on port `9000` with the same credentials,
region, and bucket. For Chrome, allow cross-origin S3 requests from the local
wasm-bindgen test runner.

## Run the tests

```sh
cd core/edge/s3_read_on_wasm
wasm-pack test --node
wasm-pack test --headless --chrome
```

Each command runs `test_hello_world` once and skips the test target for the other
runtime. Run `wasm-pack build` from the same directory to build the example
package.

If you started Silo with Docker Compose, stop it from the repository root.

```sh
docker compose -f fixtures/s3/docker-compose-silo.yml down
```
