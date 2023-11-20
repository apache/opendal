# OpenDAL Benchmark VS AWS SDK S3

This benchmark compares the performance of OpenDAL with the performance of the `aws_sdk_s3`.

## Goal

We expect OpenDAL to match `aws_sdk_s3` in speed: the throughput of OpenDAL should be within a `5%` range of `aws_sdk_s3`.

## Notes

Please run bench case separately, otherwise the result will be affected by each other since we are sharing the same runtime.

## Usage

For test: `cargo run`

```shell
> cargo run
Testing read/opendal_s3_reader
Success
Testing read/aws_s3_sdk_into_async_read
Success
Testing read/opendal_s3_reader_with_capacity
Success
Testing read/aws_s3_sdk_into_async_read_with_capacity
Success
```

For bench case: `cargo run --release -- opendal_s3_reader --bench`

```shell
> cargo run --release -- opendal_s3_reader --bench
read/opendal_s3_reader  time:   [12.773 ms 13.004 ms 13.232 ms]
                        thrpt:  [1.1809 GiB/s 1.2016 GiB/s 1.2232 GiB/s]
```

For bench: `cargo run --release -- --bench`

```shell
> cargo run --release -- --bench
read/opendal_s3_reader  time:   [12.773 ms 13.004 ms 13.232 ms]
                        thrpt:  [1.1809 GiB/s 1.2016 GiB/s 1.2232 GiB/s]

read/aws_s3_sdk_into_async_read
                        time:   [12.527 ms 12.842 ms 13.158 ms]
                        thrpt:  [1.1875 GiB/s 1.2168 GiB/s 1.2473 GiB/s]

read/opendal_s3_reader_with_capacity
                        time:   [9.6098 ms 9.8133 ms 10.017 ms]
                        thrpt:  [1.5599 GiB/s 1.5922 GiB/s 1.6259 GiB/s]

read/aws_s3_sdk_into_async_read_with_capacity
                        time:   [9.8970 ms 10.113 ms 10.329 ms]
                        thrpt:  [1.5128 GiB/s 1.5451 GiB/s 1.5788 GiB/s]
```
