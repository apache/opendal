# OpenDAL Benchmark VS Fs

This benchmark compares the performance of OpenDAL with the performance of the `std::fs`.

## Goal

We expect OpenDAL to match `std::fs` in speed: the throughput of OpenDAL should be within a `5%` range of `std::fs`.

## Usage

For test: `cargo run`

```shell
Testing vs_fs/std_fs_read
Success
Testing vs_fs/opendal_fs_read
Success
Testing vs_fs/opendal_fs_read_with_range
Success
```

For bench: `cargo run --release -- --bench`

```shell
read/std_fs       time:   [749.57 µs 762.69 µs 777.07 µs]
                  thrpt:  [20.108 GiB/s 20.487 GiB/s 20.845 GiB/s]
                        
read/opendal_fs   time:   [750.90 µs 755.39 µs 760.49 µs]
                  thrpt:  [20.546 GiB/s 20.685 GiB/s 20.808 GiB/s]
                        
read/opendal_fs_with_range
                  time:   [684.02 µs 690.77 µs 697.99 µs]
                  thrpt:  [22.386 GiB/s 22.620 GiB/s 22.843 GiB/s]

```
