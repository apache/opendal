# Upgrade

This document intends to record upgrade and migrate procedures while OpenDAL meets breaking changes.

## From v0.3 to v0.4

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

Instead, OpenDAL are providing more general `range_reader` powered by `RangeBounds`:

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

Since v0.4, `Credential` has been removed, please use API provided by `Builder` directly.

```rust
builder.access_key_id("access_key_id");
builder.secret_access_key("secret_access_key");
```

### Write returns `BytesWriter` instead

Since v0.4, `Accessor::write` will return a `BytesWriter` instead accepting a `BoxedAsyncReader`.

Along with this change, old `Writer` has been replaced by new set of write functions:

```rust
pub async fn write(&self, bs: impl AsRef<[u8]>) -> Result<()> {}
pub async fn writer(&self, size: u64) -> Result<impl BytesWrite> {}
```

Users can write into object more easily:

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

For better naming, types that returned by OpenDAL have been renamed:

- `AsyncRead + Unpin + Send` => `BytesRead`
- `BoxedAsyncReader` => `BytesReader`
- `AsyncWrite + Unpin + Send` => `BytesWrite`
- `BoxedAsyncWriter` => `BytesWriter`
- `ObjectStream` => `ObjectStreamer`