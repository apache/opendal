# OpenDAL Node.js Bindings Benchmark

This benchmark is test against the opendal and aws js sdk.

To run the benchmark, please make sure the following env have been set correctly.

- AWS_S3_ENDPOINT: the endpoint of the s3 service
- AWS_S3_REGION: the region of the s3 service
- AWS_ACCESS_KEY_ID: the access key of the s3 service
- AWS_SECRET_ACCESS_KEY: the secret key of the s3 service
- AWS_BUCKET: the bucket name of the s3 service

To run the benchmark:

```shell
pnpm bench
```
