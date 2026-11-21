# Apache OpenDAL™ Hedge Layer

`opendal-layer-hedge` issues an additional attempt when a stat or read request is still pending after a configured delay. This can reduce tail latency at the cost of extra requests.

## Use through `opendal`

```shell
cargo add opendal --features layers-hedge
```

Pass `opendal::layers::HedgeLayer::new()` to `Operator::layer` to enable hedging.

## Use with `opendal-core`

```shell
cargo add opendal-core opendal-layer-hedge
```

Import `opendal_layer_hedge::HedgeLayer` when using the split crates directly.

## Documentation

- [Rust API documentation](https://docs.rs/opendal-layer-hedge)
- [Apache OpenDAL documentation](https://opendal.apache.org/docs/)

## License

Licensed under the Apache License, Version 2.0.
