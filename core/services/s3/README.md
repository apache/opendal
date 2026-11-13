# Apache OpenDAL™ Amazon S3 Service

`opendal-service-s3` provides access to Amazon S3 and S3-compatible object storage for
applications built with Apache OpenDAL™. It includes provider presets for Cloudflare R2
and MinIO.

## Use through `opendal`

Applications should enable the service or provider they use through its
matching `opendal` facade feature:

```shell
# Generic S3
cargo add opendal --features services-s3
# Cloudflare R2
cargo add opendal --features services-r2
# MinIO
cargo add opendal --features services-minio
```

The matching builders are available as `opendal::services::S3`,
`opendal::services::R2`, and `opendal::services::Minio`. Configure a builder,
then pass it to `opendal::Operator::new`.

## Use with `opendal-core`

Add the split crates directly:

```shell
cargo add opendal-core opendal-service-s3
```

Pass a configured service builder to `Operator::new`:

```rust
use opendal_core::{Operator, OperatorRegistry, Result};
use opendal_service_s3::{register_s3_service, S3};

fn build_operator(builder: S3) -> Result<Operator> {
    Operator::new(builder)
}

fn register_for_uri() {
    register_s3_service(OperatorRegistry::get());
}
```

`register_for_uri` registers only the `s3` scheme. Use
`register_r2_service` or `register_minio_service` for the corresponding
provider scheme. Registration is only needed for scheme-driven construction
through `Operator::from_uri` or `Operator::via_iter`.

Services that send HTTP requests also require an HTTP transport in
`OperationContext`. See the
[`opendal-core` composition example](https://crates.io/crates/opendal-core).

## Documentation

- [Service configuration and examples](https://opendal.apache.org/services/s3)
- [Cloudflare R2 preset](https://opendal.apache.org/services/r2)
- [MinIO preset](https://opendal.apache.org/services/minio)
- [Rust API documentation](https://docs.rs/opendal-service-s3)
- [Apache OpenDAL documentation](https://opendal.apache.org/docs/)

## License

Licensed under the Apache License, Version 2.0.
