# Apache OpenDAL™ Sanity Check Layer

`opendal-layer-sanity-check` detects duplicate entries in listing responses.

Enable the `layers-sanity-check` feature in `opendal` and apply
`opendal::layers::SanityCheckLayer` with `Operator::layer`.

Use the following imports when composing the split crates directly.

```rust
use opendal_core::Operator;
use opendal_layer_sanity_check::SanityCheckLayer;

fn add_sanity_checks(operator: Operator) -> Operator {
    operator.layer(SanityCheckLayer)
}
```

## License

Licensed under the Apache License, Version 2.0.
