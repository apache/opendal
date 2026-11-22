# Apache OpenDAL™ WebDAV Service

`opendal-service-webdav` provides access to storage over WebDAV for applications built
with Apache OpenDAL™.

> **Warning:** Verify conditional writes and deletes against your endpoint before
> relying on them. In the tested configurations, nginx’s `ngx_http_dav_module`
> ignored PUT preconditions and `If-Match` on DELETE. Apache `mod_dav_fs` allowed
> concurrent `If-None-Match: *` creates to succeed and allowed an overwrite with
> a matching weak `If-None-Match`. Nextcloud and ownCloud could reuse an ETag
> after a same-second overwrite, allowing a stale `If-Match` to overwrite or
> delete newer content.
>
> Use `CapabilityOverrideLayer` to disable the affected capabilities. OpenDAL
> returns `ErrorKind::Unsupported` before sending a conditional write or delete
> to the server if the operation uses a disabled capability.

## Use through `opendal`

Applications should normally enable this service through the `opendal` facade with the
`services-webdav` feature:

```shell
cargo add opendal --features services-webdav
```

The service is available as `opendal::services::Webdav`. Configure the
service builder, then pass it to `opendal::Operator::new`.

## Use with `opendal-core`

Add the split crates directly:

```shell
cargo add opendal-core opendal-service-webdav
```

Pass a configured service builder to `Operator::new`:

```rust
use opendal_core::{Operator, OperatorRegistry, Result};
use opendal_service_webdav::{register_webdav_service, Webdav};

fn build_operator(builder: Webdav) -> Result<Operator> {
    Operator::new(builder)
}

fn register_for_uri() {
    register_webdav_service(OperatorRegistry::get());
}
```

`register_for_uri` is only needed for scheme-driven construction through
`Operator::from_uri` or `Operator::via_iter`.

Services that send HTTP requests also require an HTTP transport in
`OperationContext`. See the
[`opendal-core` composition example](https://crates.io/crates/opendal-core).

## Documentation

- [Service configuration and examples](https://opendal.apache.org/services/webdav)
- [Rust API documentation](https://docs.rs/opendal-service-webdav)
- [Apache OpenDAL documentation](https://opendal.apache.org/docs/)

## License

Licensed under the Apache License, Version 2.0.
