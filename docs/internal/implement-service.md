# Implement service

Implement a service mainly two parts:

- `Backend`: the struct that implement [`Accessor`](/opendal/trait.Accessor.html) trait.
- `Builder`: the struct that provide `fn build(&mut self) -> Result<Backend>`.

## Backend

Backend needs to implement `Accessor`:

```rust
#[async_trait]
impl Accessor for Backend {}
```

### Metadata

The only function that required to implement is `metadata`:

```rust
#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Xxx)
            .set_root(&self.root)
            .set_capabilities(AccessorCapability::Read);
        am
    }
}
```

In this function, backend needs to return:

- `scheme`: The [`Scheme`](/opendal/enum.Scheme.html) of this backend.
- `root`: The root path of current backend.
- `capabilities`: The capabilities that this backend have.
- `name`: The name of this backend (for object storage services, it's bucket name)

Available capabilities including:

- `Read`: Set this capability if this backend can `read` and `stat`.
- `Write`: Set this capability if this backend can `write` and `delete`.
- `List`: Set this capability if this backend can `list`.
- `Presign`: Set this capability if this backend can `presign`.
- `Multipart`: Set this capability if this backend can maintain multipart.
- `Blocking`: Set this capability if this backend can be used in blocking context.

## Builder

`Builder` must implement `Default` and the following functions:

```rust
impl Builder {
    pub fn build(&mut self) -> Result<Backend> {}
}
```

Builder's API is part of OpenDAL public API, please don't mark function as `pub` unless needed.
