# OpenDAL object_store Binding

This crate intends to build an [object_store](https://crates.io/crates/object_store) binding.

The `OpendalStore` uses the `opendal` crate to interact with the underlying object storage system. The `Operator` is used to create a new `OpendalStore` instance. The `OpendalStore` uses the `Operator` to perform operations on the object storage system, such as `put`, `get`, `delete`, and `list`.

## Examples

first you need to add the following dependencies to your `Cargo.toml`:

```toml
[dependencies]
object_store = "0.9.0"
object_store_opendal = "0.43.1"
opendal = { version = "0.46.0", features = ["services-s3"] }
```

> the current version we support is object store 0.9

Then you can use the `OpendalStore` as follows:

```rust
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{services::S3, Builder, Operator};

async fn operate_object_store() {
    let builder = S3::from_map(
        vec![
            ("access_key".to_string(), "my_access_key".to_string()),
            ("secret_key".to_string(), "my_secret_key".to_string()),
            ("endpoint".to_string(), "my_endpoint".to_string()),
            ("region".to_string(), "my_region".to_string()),
        ]
        .into_iter()
        .collect(),
    );

    // Create a new operator
    let operator = Operator::new(builder).unwrap().finish();

    // Create a new object store
    let object_store = Arc::new(OpendalStore::new(operator));

    let path = Path::from("data/nested/test.txt");
    let bytes = Bytes::from_static(b"hello, world! I am nested.");

    object_store.put(&path, bytes.clone().into()).await.unwrap();

    let content = object_store.get(&path).await.unwrap().bytes().await.unwrap();

    assert_eq!(content, bytes);
}
```

## API

The `OpendalStore` implements the `ObjectStore` trait, which provides the following methods:

| Method | Description | link |
| --- | --- | --- |
| `put` | Put an object into the store. | [put](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.put) |
| `get` | Get an object from the store. | [get](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.get) |
| `get_range` | Get a range of bytes from an object in the store. | [get_range](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.get_range) |
| `head` | Get the metadata of an object in the store. | [head](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.head) |
| `delete` | Delete an object from the store. | [delete](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.delete) |
| `list` | List objects in the store. | [list](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.list) |
| `list_with_offset` | List objects in the store with an offset. | [list_with_offset](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.list_with_offset) |
| `list_with_delimiter` | List objects in the store with a delimiter. | [list_with_delimiter](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.list_with_delimiter) |
