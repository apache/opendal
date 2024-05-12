# Types Benchmark Tests

This benchmark contains performance testing of critical data structures in opendal types, currently including performance testing of Buffer.

## Run

```shell
cargo bench --bench types --features tests
```

The following are the test results for reference:

```shell
bench_contiguous_buffer/bytes buf 256 KiB chunk
                        time:   [210.54 ps 211.54 ps 212.62 ps]
bench_contiguous_buffer/contiguous buffer 256 KiB chunk
                        time:   [233.12 ps 233.90 ps 234.69 ps]
bench_contiguous_buffer/bytes buf 256 KiB advance
                        time:   [10.238 ns 10.239 ns 10.240 ns]
bench_contiguous_buffer/contiguous buffer 256 KiB advance
                        time:   [14.758 ns 14.764 ns 14.771 ns]
bench_contiguous_buffer/bytes buf 4.00 MiB chunk
                        time:   [210.26 ps 211.32 ps 212.45 ps]
bench_contiguous_buffer/contiguous buffer 4.00 MiB chunk
                        time:   [256.97 ps 257.30 ps 257.62 ps]
bench_contiguous_buffer/bytes buf 4.00 MiB advance
                        time:   [10.238 ns 10.239 ns 10.240 ns]
bench_contiguous_buffer/contiguous buffer 4.00 MiB advance
                        time:   [14.756 ns 14.760 ns 14.764 ns]

bench_non_contiguous_buffer/bytes buf 256 KiB * 4  chunk
                        time:   [250.12 ps 250.53 ps 250.97 ps]
bench_non_contiguous_buffer/non contiguous buffer 256 KiB * 4 chunk
                        time:   [307.16 ps 307.24 ps 307.34 ps]
bench_non_contiguous_buffer/bytes buf 256 KiB * 4  advance
                        time:   [10.236 ns 10.238 ns 10.241 ns]
bench_non_contiguous_buffer/non contiguous buffer 256 KiB * 4 advance
                        time:   [9.2202 ns 9.2216 ns 9.2239 ns]
bench_non_contiguous_buffer/bytes buf 256 KiB * 32  chunk
                        time:   [210.27 ps 211.64 ps 213.35 ps]
bench_non_contiguous_buffer/non contiguous buffer 256 KiB * 32 chunk
                        time:   [307.28 ps 307.34 ps 307.40 ps]
bench_non_contiguous_buffer/bytes buf 256 KiB * 32  advance
                        time:   [10.239 ns 10.241 ns 10.244 ns]
bench_non_contiguous_buffer/non contiguous buffer 256 KiB * 32 advance
                        time:   [9.2201 ns 9.2214 ns 9.2235 ns]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 4  chunk
                        time:   [210.68 ps 211.47 ps 212.35 ps]
bench_non_contiguous_buffer/non contiguous buffer 4.00 MiB * 4 chunk
                        time:   [307.11 ps 307.14 ps 307.16 ps]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 4  advance
                        time:   [10.240 ns 10.242 ns 10.245 ns]
bench_non_contiguous_buffer/non contiguous buffer 4.00 MiB * 4 advance
                        time:   [9.2179 ns 9.2200 ns 9.2233 ns]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 32  chunk
                        time:   [210.61 ps 211.60 ps 212.65 ps]
bench_non_contiguous_buffer/non contiguous buffer 4.00 MiB * 32 chunk
                        time:   [307.34 ps 307.36 ps 307.39 ps]
bench_non_contiguous_buffer/bytes buf 4.00 MiB * 32  advance
                        time:   [10.244 ns 10.244 ns 10.245 ns]
bench_non_contiguous_buffer/non contiguous buffer 4.00 MiB * 32 advance
                        time:   [9.2168 ns 9.2186 ns 9.2212 ns]
```
