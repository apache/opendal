# Apache OpenDAL™ Amazon S3 Service

`opendal-service-s3` provides access to Amazon S3 and S3-compatible object storage for
applications built with Apache OpenDAL™. Provider presets supply documented endpoints,
signing regions, and bucket addressing through the same implementation.

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

Configure the matching builder from `opendal::services` and pass it to
`opendal::Operator::new`. Each provider has its own `services-<scheme>`
feature. See the links below for provider configuration and examples.

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

`register_for_uri` registers only the `s3` scheme. Call the corresponding
`register_<scheme>_service` function to register a provider preset.
Registration is only needed for scheme-driven construction through
`Operator::from_uri` or `Operator::via_iter`.

Services that send HTTP requests also require an HTTP transport in
`OperationContext`. See the
[`opendal-core` composition example](https://crates.io/crates/opendal-core).

## Documentation

- [Service configuration and examples](https://opendal.apache.org/services/s3)
- [Cloudflare R2 preset](https://opendal.apache.org/services/r2)
- [MinIO preset](https://opendal.apache.org/services/minio)
- [Exoscale Simple Object Storage preset](https://opendal.apache.org/services/exoscale)
- [Hetzner Object Storage preset](https://opendal.apache.org/services/hetzner)
- [Oracle Cloud Infrastructure Object Storage preset](https://opendal.apache.org/services/oci)
- [OVHcloud Object Storage preset](https://opendal.apache.org/services/ovhcloud)
- [QingStor Object Storage preset](https://opendal.apache.org/services/qingstor)
- [Scaleway Object Storage preset](https://opendal.apache.org/services/scaleway)
- [DigitalOcean Spaces preset](https://opendal.apache.org/services/spaces)
- [Supabase Storage preset](https://opendal.apache.org/services/supabase)
- [Tigris preset](https://opendal.apache.org/services/tigris)
- [Wasabi Object Storage preset](https://opendal.apache.org/services/wasabi)
- [Rust API documentation](https://docs.rs/opendal-service-s3)
- [Apache OpenDAL documentation](https://opendal.apache.org/docs/)

## License

Licensed under the Apache License, Version 2.0.
