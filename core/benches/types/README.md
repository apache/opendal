# Types Benchmark Tests

This benchmark contains performance testing of critical data structures in opendal types, currently including performance testing of Buffer.

## Run

```shell
cargo bench --bench types --features tests
```

The following are the test results for reference:

```shell
bench_non_contiguous_buffer/bytes buf 256 KiB * 4  chunk
                        time:   [226.31 ps 227.81 ps 229.33 ps]
bench_non_contiguous_buffer/buffer 256 KiB * 4 chunk
                        time:   [319.85 ps 325.66 ps 332.61 ps]
bench_non_contiguous_buffer/bytes buf 256 KiB * 4  advance
                        time:   [10.241 ns 10.242 ns 10.243 ns]
bench_non_contiguous_buffer/buffer 256 KiB * 4 advance
                        time:   [9.2208 ns 9.2216 ns 9.2226 ns]
bench_non_contiguous_buffer/bytes buf 256 KiB * 4  truncate
                        time:   [10.248 ns 10.253 ns 10.258 ns]
bench_non_contiguous_buffer/buffer 256 KiB * 4 truncate
                        time:   [8.8055 ns 8.8068 ns 8.8085 ns]
bench_non_contiguous_buffer/bytes buf 256 KiB * 32  chunk
                        time:   [228.76 ps 230.04 ps 231.29 ps]
bench_non_contiguous_buffer/buffer 256 KiB * 32 chunk
                        time:   [319.49 ps 323.25 ps 327.83 ps]
bench_non_contiguous_buffer/bytes buf 256 KiB * 32  advance
                        time:   [10.244 ns 10.245 ns 10.246 ns]
bench_non_contiguous_buffer/buffer 256 KiB * 32 advance
                        time:   [9.2172 ns 9.2184 ns 9.2197 ns]
bench_non_contiguous_buffer/bytes buf 256 KiB * 32  truncate
                        time:   [10.242 ns 10.243 ns 10.246 ns]
bench_non_contiguous_buffer/buffer 256 KiB * 32 truncate
                        time:   [8.8071 ns 8.8089 ns 8.8118 ns]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 4  chunk
                        time:   [222.45 ps 223.42 ps 224.47 ps]
bench_non_contiguous_buffer/buffer 4.00 MiB * 4 chunk
                        time:   [364.31 ps 373.49 ps 382.43 ps]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 4  advance
                        time:   [10.243 ns 10.244 ns 10.245 ns]
bench_non_contiguous_buffer/buffer 4.00 MiB * 4 advance
                        time:   [9.2196 ns 9.2204 ns 9.2213 ns]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 4  truncate
                        time:   [10.244 ns 10.245 ns 10.246 ns]
bench_non_contiguous_buffer/buffer 4.00 MiB * 4 truncate
                        time:   [8.8083 ns 8.8094 ns 8.8105 ns]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 32  chunk
                        time:   [229.61 ps 230.68 ps 231.71 ps]
bench_non_contiguous_buffer/buffer 4.00 MiB * 32 chunk
                        time:   [364.61 ps 369.01 ps 373.13 ps]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 32  advance
                        time:   [10.239 ns 10.240 ns 10.242 ns]
bench_non_contiguous_buffer/buffer 4.00 MiB * 32 advance
                        time:   [9.2188 ns 9.2238 ns 9.2336 ns]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 32  truncate
                        time:   [10.249 ns 10.265 ns 10.291 ns]
bench_non_contiguous_buffer/buffer 4.00 MiB * 32 truncate
                        time:   [8.8080 ns 8.8092 ns 8.8106 ns]

bench_non_contiguous_buffer_with_extreme/256 KiB * 1k chunk
                        time:   [352.06 ps 361.52 ps 371.51 ps]
bench_non_contiguous_buffer_with_extreme/256 KiB * 1k advance
                        time:   [378.80 ns 378.97 ns 379.18 ns]
bench_non_contiguous_buffer_with_extreme/256 KiB * 1k truncate
                        time:   [8.8039 ns 8.8049 ns 8.8061 ns]
bench_non_contiguous_buffer_with_extreme/256 KiB * 10k chunk
                        time:   [318.90 ps 320.87 ps 322.97 ps]
bench_non_contiguous_buffer_with_extreme/256 KiB * 10k advance
                        time:   [3.6598 µs 3.6613 µs 3.6634 µs]
bench_non_contiguous_buffer_with_extreme/256 KiB * 10k truncate
                        time:   [8.8065 ns 8.8074 ns 8.8083 ns]
bench_non_contiguous_buffer_with_extreme/256 KiB * 100k chunk
                        time:   [319.32 ps 326.56 ps 334.76 ps]
bench_non_contiguous_buffer_with_extreme/256 KiB * 100k advance
                        time:   [40.561 µs 40.623 µs 40.690 µs]
bench_non_contiguous_buffer_with_extreme/256 KiB * 100k truncate
                        time:   [8.8071 ns 8.8081 ns 8.8092 ns]
bench_non_contiguous_buffer_with_extreme/256 KiB * 1000k chunk
                        time:   [322.26 ps 329.02 ps 336.15 ps]
bench_non_contiguous_buffer_with_extreme/256 KiB * 1000k advance
                        time:   [848.22 µs 848.97 µs 849.64 µs]
bench_non_contiguous_buffer_with_extreme/256 KiB * 1000k truncate
                        time:   [8.8061 ns 8.8073 ns 8.8086 ns]
```
