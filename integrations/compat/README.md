# Apache OpenDAL™ Compat integration

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal/ci_integration_compat.yml?branch=main
[actions]: https://github.com/apache/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/opendal_compat.svg
[crates.io]: https://crates.io/crates/opendal_compat
[crate downloads]: https://img.shields.io/crates/d/opendal_compat.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

`opendal-compat` provides compatibility functions for opendal.

This crate can make it easier to resolve the compatibility issues between different versions of opendal.

## Useful Links

- Documentation: [release](https://docs.rs/opendal_compat/) | [dev](https://opendal.apache.org/docs/opendal_compat/opendal_compat/)

## Examples

Add the following dependencies to your `Cargo.toml` with correct version:

```toml
[dependencies]
opendal_compat = { version = "1", features = ["v0_50_to_v0_49"] }
opendal = { version = "0.50.0" }
opendal_v0_49 = { package="opendal", version = "0.49" }
```

Convert `opendal::Operator` to old opendal `Operator`:

```rust
use opendal_v0_50::Operator;
use opendal_v0_50::services::MemoryConfig;
use opendal_v0_50::Result;

fn i_need_opendal_v0_49_op(op: opendal_v0_49::Operator) {
   // do something with old opendal;
}

fn main() -> Result<()> {
    let v0_50_op = Operator::from_config(MemoryConfig::default())?.finish();
    let v0_49_op = opendal_compat::v0_50_to_v0_49(v0_50_op);
    i_need_opendal_v0_49_op(v0_49_op);
    Ok(())
}
```

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
