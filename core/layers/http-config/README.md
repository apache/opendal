# Apache OpenDAL™ HTTP Config Layer

`opendal-layer-http-config` replaces an operator's HTTP transport with one built
from a portable `HttpConfig` value.

## Use through `opendal`

Applications should normally enable this layer through the `opendal` facade with the
`layers-http-config` feature:

```shell
cargo add opendal --features layers-http-config
```

The layer is available as `opendal::layers::HttpConfigLayer`, and `HttpConfig` is
available as `opendal::HttpConfig`. Compose it with an operator through
`opendal::Operator::layer`.

## Use with `opendal-core`

Add the split crates directly:

```shell
cargo add opendal-core opendal-layer-http-config
```

Build a configured layer and pass it to `Operator::layer`:

```rust
use opendal_core::HttpConfig;
use opendal_core::Operator;
use opendal_layer_http_config::HttpConfigLayer;

fn add_layer(operator: Operator, config: HttpConfig) -> opendal_core::Result<Operator> {
    Ok(operator.layer(HttpConfigLayer::new(config)?))
}
```

The layer is created through `HttpConfigLayer::new`, which resolves the
configured transport immediately. Invalid options fail when the layer is created
instead of on the first request.

## Documentation

- [Rust API documentation](https://docs.rs/opendal-layer-http-config)
- [Apache OpenDAL documentation](https://opendal.apache.org/docs/)

## License

Licensed under the Apache License, Version 2.0.
