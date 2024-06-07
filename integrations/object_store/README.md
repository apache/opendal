# OpenDAL object_store Binding

This crate intends to build an [object_store](https://crates.io/crates/object_store) binding.

The `OpendalStore` uses the `opendal` crate to interact with the underlying object storage system. The `Operator` is used to create a new `OpendalStore` instance. The `OpendalStore` uses the `Operator` to perform operations on the object storage system, such as `put`, `get`, `delete`, and `list`.

## Examples

Firstly, you need to add the following dependencies to your `Cargo.toml`:

```toml
[dependencies]
object_store = "0.10.0"
object_store_opendal = "0.44.0"
opendal = { version = "0.47.0", features = ["services-s3"] }
```

> [!NOTE]
> 
> The current version we support is object store 0.10.

Then you can use the `OpendalStore` as in the [example](examples/basic.rs).

## API

The `OpendalStore` implements the `ObjectStore` trait, which provides the following methods:

| Method                | Description                                       | link                                                                                                                       |
|-----------------------|---------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `put`                 | Put an object into the store.                     | [put](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.put)                                 |
| `get`                 | Get an object from the store.                     | [get](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.get)                                 |
| `get_range`           | Get a range of bytes from an object in the store. | [get_range](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.get_range)                     |
| `head`                | Get the metadata of an object in the store.       | [head](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.head)                               |
| `delete`              | Delete an object from the store.                  | [delete](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.delete)                           |
| `list`                | List objects in the store.                        | [list](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.list)                               |
| `list_with_offset`    | List objects in the store with an offset.         | [list_with_offset](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.list_with_offset)       |
| `list_with_delimiter` | List objects in the store with a delimiter.       | [list_with_delimiter](https://docs.rs/object_store/0.9.0/object_store/trait.ObjectStore.html#tymethod.list_with_delimiter) |
