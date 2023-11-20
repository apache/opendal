# OpenDAL Benchmark VS AWS SDK S3

This benchmark compares the performance of OpenDAL with the performance of the `aws_sdk_s3`.

## Goal

We expect OpenDAL to match `aws_sdk_s3` in speed: the throughput of OpenDAL should be within a `10%` range of `aws_sdk_s3`.

## Usage

For test: `cargo run`

```shell
Testing read/aws_s3_sdk_collect
Success
Testing read/aws_s3_sdk_into_async_read
Success
Testing read/aws_s3_sdk_into_async_read_with_capacity
Success
Testing read/opendal_s3
Success
Testing read/opendal_s3_with_capacity
Success
```

For bench: `cargo run --release -- --bench`

```shell
read/aws_s3_sdk_collect time:   [47.264 ms 47.378 ms 47.504 ms]
                        thrpt:  [336.82 MiB/s 337.71 MiB/s 338.53 MiB/s]

read/aws_s3_sdk_into_async_read
                        time:   [9.8422 ms 11.607 ms 13.703 ms]
                        thrpt:  [1.1403 GiB/s 1.3462 GiB/s 1.5876 GiB/s]

read/aws_s3_sdk_into_async_read_with_size_known
                        time:   [7.9572 ms 8.1055 ms 8.2552 ms]
                        thrpt:  [1.8927 GiB/s 1.9277 GiB/s 1.9636 GiB/s]

read/opendal_s3         time:   [8.9068 ms 9.2614 ms 9.6912 ms]
                        thrpt:  [1.6123 GiB/s 1.6871 GiB/s 1.7543 GiB/s]

read/opendal_s3_with_range
                        time:   [8.5459 ms 8.7592 ms 8.9739 ms]
                        thrpt:  [1.7412 GiB/s 1.7838 GiB/s 1.8284 GiB/s]
```
