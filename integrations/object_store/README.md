# Apache OpenDAL™ object_store integration

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal/ci_integration_object_store.yml?branch=main
[actions]: https://github.com/apache/opendal/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/object_store_opendal.svg
[crates.io]: https://crates.io/crates/object_store_opendal
[crate downloads]: https://img.shields.io/crates/d/object_store_opendal.svg
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

`object_store_opendal` is an [`object_store`](https://crates.io/crates/object_store) implementation using [`opendal`](https://github.com/apache/opendal).

This crate can help you to access 30 more storage services with the same object_store API.


## Useful Links

- Documentation: [release](https://docs.rs/object_store_opendal/) | [dev](https://opendal.apache.org/docs/object-store-opendal/object_store_opendal/)

## Examples

Add the following dependencies to your `Cargo.toml` with correct version:

```toml
[dependencies]
object_store = "0.11.0"
object_store_opendal = "0.47.0"
opendal = { version = "0.49.0", features = ["services-s3"] }
```

Build `OpendalStore` via `opendal::Operator`:

```rust
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::services::S3;
use opendal::{Builder, Operator};

#[tokio::main]
async fn main() {
    let builder = S3::from_map(
        vec![
            ("access_key".to_string(), "my_access_key".to_string()),
            ("secret_key".to_string(), "my_secret_key".to_string()),
            ("endpoint".to_string(), "my_endpoint".to_string()),
            ("region".to_string(), "my_region".to_string()),
        ]
        .into_iter()
        .collect(),
    ).unwrap();

    // Create a new operator
    let operator = Operator::new(builder).unwrap().finish();

    // Create a new object store
    let object_store = Arc::new(OpendalStore::new(operator));

    let path = Path::from("data/nested/test.txt");
    let bytes = Bytes::from_static(b"hello, world! I am nested.");

    object_store.put(&path, bytes.clone().into()).await.unwrap();

    let content = object_store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(content, bytes);
}
```

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
