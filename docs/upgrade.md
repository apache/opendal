# Upgrade

This document intends to record upgrade and migrate procedures while OpenDAL meets breaking changes.

## Upgraded to v0.12

OpenDAL introduces breaking changes for services initiation.

Since v0.12, `Operator::new` will accept `impl Accessor + 'static` instead of `Arc<dyn Accessor>`:

```rust
impl Operator {
    pub fn new(accessor: impl Accessor + 'static) -> Self { .. }
}
```

Every service's `Builder` now have a `build()` API which can be run without async:

```rust
let mut builder = fs::Builder::default();
let op: Operator = Operator::new(builder.build()?);
```

Along with these changes, `Operator::from_iter` and `Operator::from_env` now is a blocking API too.

The following APIs have been deprecated:

- All services `Builder::finish()` (replaced by `Builder::build()`)
- All services `Backend::build()` (replace by `Builder::default()`)

The following APIs have been removed:

- public struct `Metadata` (deprecated in v0.8, replaced by `ObjectMetadata`)

## Upgraded to v0.8

OpenDAL introduces a breaking change of `list` related operations in v0.8.

Since v0.8, `list` will return `DirStreamer` instead:

```rust
pub trait Accessor: Send + Sync + Debug {
    async fn list(&self, args: &OpList) -> Result<DirStreamer> {}
}
```

`DirStreamer` streams `DirEntry` which carries `ObjectMode`, so that we don't need an extra call to get object mode:

```rust
impl DirEntry {
    pub fn mode(&self) -> ObjectMode {
        self.mode
    }
}
```

And `DirEntry` can be converted into `Object` without overhead:

```rust
let o: Object = de.into()
```

Since `v0.8`, `opendal::Metadata` has been deprecated by `opendal::ObjectMetadata`.

## Upgraded to v0.7

OpenDAL introduces a breaking change of `decompress_read` related in v0.7.

Since v0.7, `decompress_read` and `decompress_reader` will return `Ok(None)` while OpenDAL can't detect the correct compress algorithm.

```rust
impl Object {
    pub async fn decompress_read(&self) -> Result<Option<Vec<u8>>> {}
    pub async fn decompress_reader(&self) -> Result<Option<impl BytesRead>> {}
}
```

So users should match and check the `None` case:

```rust
let bs = o.decompress_read().await?.expect("must have valid compress algorithm");
```

## Upgraded to v0.4

OpenDAL introduces many breaking changes in v0.4.

### Object::reader() is not `AsyncSeek` anymore

Since v0.4, `Object::reader()` will return `impl BytesRead` instead of `Reader` that implements `AsyncRead` and `AsyncSeek`. Users who want `AsyncSeek` please wrapped with `opendal::io_util::seekable_read`:

```rust
use opendal::io_util::seekable_read;

let o = op.object("test");
let mut r = seekable_read(&o, 10..);
r.seek(SeekFrom::Current(10)).await?;
let mut bs = vec![0;10];
r.read(&mut bs).await?;
```

### Use RangeBounds instead

Since v0.4, the following APIs will be removed.

- `Object::limited_reader(size: u64)`
- `Object::offset_reader(offset: u64)`
- `Object::range_reader(offset: u64, size: u64)`

Instead, OpenDAL is providing a more general `range_reader` powered by `RangeBounds`:

```rust
pub async fn range_reader(&self, range: impl RangeBounds<u64>) -> Result<impl BytesRead>
```

Users can use their familiar rust range syntax:

```rust
let r = o.range_reader(1024..2048).await?;
```

### Return io::Result instead

Since v0.4, all functions in OpenDAL will return `std::io::Result` instead.

Please check via `std::io::ErrorKind` directly:

```rust
use std::io::ErrorKind;

if let Err(e) = op.object("test_file").metadata().await {
    if e.kind() == ErrorKind::NotFound {
        println!("object not exist")
    }
}
```

### Removing Credential

Since v0.4, `Credential` has been removed, please use the API provided by `Builder` directly.

```rust
builder.access_key_id("access_key_id");
builder.secret_access_key("secret_access_key");
```

### Write returns `BytesWriter` instead

Since v0.4, `Accessor::write` will return a `BytesWriter` instead accepting a `BoxedAsyncReader`.

Along with this change, the old `Writer` has been replaced by a new set of write functions:

```rust
pub async fn write(&self, bs: impl AsRef<[u8]>) -> Result<()> {}
pub async fn writer(&self, size: u64) -> Result<impl BytesWrite> {}
```

Users can write into an object more easily:

```rust
let _ = op.object("path/to/file").write("Hello, World!").await?;
```

### `io_util` replaces `readers`

Since v0.4, mod `io_util` will replace `readers`. In `io_utils`, OpenDAL provides helpful functions like:

- `into_reader`: Convert `BytesStream` into `BytesRead`
- `into_sink`: Convert `BytesWrite` into `BytesSink`
- `into_stream`: Convert `BytesRead` into `BytesStream`
- `into_writer`: Convert `BytesSink` into `BytesWrite`
- `observe_read`: Add callback for `BytesReader`
- `observe_write`: Add callback for `BytesWrite`

### New type alias

For better naming, types that OpenDAL returns have been renamed:

- `AsyncRead + Unpin + Send` => `BytesRead`
- `BoxedAsyncReader` => `BytesReader`
- `AsyncWrite + Unpin + Send` => `BytesWrite`
- `BoxedAsyncWriter` => `BytesWriter`
- `ObjectStream` => `ObjectStreamer`
