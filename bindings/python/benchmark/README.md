# OpenDAL Python Bindings Benchmark

This benchmark is test against the opendal and aws python sdk.

To run the benchmark, please make sure the following env have been set correctly.

- AWS_REGION: the region of the s3 service
- AWS_ENDPOINT: the endpoint of the s3 service
- AWS_ACCESS_KEY_ID: the access key of the s3 service
- AWS_SECRET_ACCESS_KEY: the secret key of the s3 service
- AWS_S3_BUCKET: the bucket name of the s3 service

To run the benchmark:

```shell
maturin develop -r -E=benchmark

export AWS_ENDPOINT=http://127.0.0.1:9000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_S3_BUCKET=opendal

uv run async_opendal_benchmark.py
uv run async_origin_s3_benchmark_with_gevent.py
```
