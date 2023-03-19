# Contributing

## Get Started

This is a Rust project, so [rustup](https://rustup.rs/) is the best place to start.

## Build

```shell
just build_core
```

## Test

```shell
just test_core
```

Behavior Tests are used to make sure every service works correctly.

```shell
cp .env.example .env
just test_core
```

Please visit [Behavior Test README](./tests/behavior/README.md) for more details.

## Benchmark

We use Ops Benchmark Tests to measure every operation's performance on the target platform.

```shell
# Setup env
cp .env.example .env
# Run benches
cargo bench
```

Please visit [Ops Benchmark README](./benches/ops/README.md) for more details.
