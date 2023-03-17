# OpenDAL Benchmarks

Build flamegraph:

```shell
cargo flamegraph --bench io --features layers-all -- seek --bench
```

- `--bench io` pick the `io` target.
- `--features layers-all` is the required features of `io`
- `-- seek --bench`: chose the benches with `seek` and `--bench` is required by `criterion`

After `flamegraph.svg` has been generated, use browser to visit it.
