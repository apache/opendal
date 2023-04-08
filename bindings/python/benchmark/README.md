# OpenDAL Python Bindings Benchmark

This benchmark is test against the opendal and aws python sdk.

To run the benchmark, please make sure the following env have been set correctly.

- AWS_S3_REGION: the region of the s3 service
- AWS_ACCESS_KEY_ID: the access key of the s3 service
- AWS_SECRET_ACCESS_KEY: the secret key of the s3 service
- AWS_BUCKET: the bucket name of the s3 service

To run the benchmark:

```shell
pip install -r requirements.txt
python async_opendal_benchmark.py
python async_origin_s3_benchmark_with_gevent.py
```
