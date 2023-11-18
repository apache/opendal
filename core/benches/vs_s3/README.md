# OpenDAL Benchmark VS AWS SDK S3

This benchmark compares the performance of OpenDAL with the performance of the `aws_sdk_s3`.

## Goal

We expect OpenDAL to match `aws_sdk_s3` in speed: the throughput of OpenDAL should be within a `5%` range of `aws_sdk_s3`.

## Usage

For test: `cargo run`

```shell
Testing read/aws_s3_sdk_collect
Success
Testing read/aws_s3_sdk_into_async_read
Success
Testing read/opendal_s3
Success
Testing read/opendal_s3_with_range
Success
```

For bench: `cargo run --release -- --bench`

```shell
read/aws_s3_sdk_collect time:   [50.007 ms 50.265 ms 50.527 ms]
                        thrpt:  [316.66 MiB/s 318.31 MiB/s 319.96 MiB/s]

read/aws_s3_sdk_into_async_read
                        time:   [11.308 ms 11.675 ms 12.046 ms]
                        thrpt:  [1.2971 GiB/s 1.3384 GiB/s 1.3818 GiB/s]

read/opendal_s3         time:   [9.5761 ms 9.8121 ms 10.050 ms]
                        thrpt:  [1.5548 GiB/s 1.5924 GiB/s 1.6317 GiB/s]

read/opendal_s3_with_range
                        time:   [8.9999 ms 9.2246 ms 9.4526 ms]
                        thrpt:  [1.6530 GiB/s 1.6938 GiB/s 1.7361 GiB/s]
```
