# Upgrade to v0.30

In version 0.30, we made significant breaking changes by removing objects. Our goal in doing so was to provide our users with APIs that are easier to understand and maintain.

More detailes could be found at [RFC: Remove Object Concept][crate::docs::rfcs::rfc_1477_remove_object_concept].

To upgrade to OpenDAL v0.30, users need to make the following changes:

- regex replace `object\((.*)\).reader\(\)` to `reader($1)`
	- replace the function on your case, it's recomanded to do it one by one
- rename `ObjectMetakey` => `Metakey`
- rename `ObjectMode` => `EntryMode`
- replace `ErrorKind::ObjectXxx` to `ErrorKind::Xxx`
- rename `AccessorMetadata` => `AccessorInfo`
- rename `ObjectMetadata` => `Metadata`
- replace `operator.metadata()` => `operator.info()`

# Upgrade to v0.29

In v0.29, we introduced [Object Writer][crate::docs::rfcs::rfc_1420_object_writer] to replace existing Multipart related APIs.

Users can now append multiparts bytes into object via:

```rust
let mut w = o.writer().await?;
w.write(bs1).await?;
w.write(bs2).await?;
w.close()
```

Along with this change, we cleaned up a lot of internal structs and traits. Users who used to depend on `opendal::raw::io::{input,output}` should use `opendal::raw::oio` instead.

Also, decompress related feature also removed. Users can use `async-compression` with `ObjectReader` directly.

# Upgrade to v0.28

In v0.28, we introduced [Query Based Metadata][crate::docs::rfcs::rfc_1398_query_based_metadata]. Users can query cached metadata with `ObjectMetakey` to make sure that OpenDAL always makes the best decision.

```diff
- pub async fn metadata(&self) -> Result<ObjectMetadata>;
+ pub async fn metadata(
+        &self,
+        flags: impl Into<FlagSet<ObjectMetakey>>,
+    ) -> Result<Arc<ObjectMetadata>>;
```

Please visit `Object::metadata()`'s example for more details.

# Upgrade to v0.27

In v0.27, we refactored our `list` related logic and added `scan` support. So make `Pager` and `BlockingPager` associated types in `Accessor` too!

```diff
pub trait Accessor: Send + Sync + Debug + Unpin + 'static {
    type Reader: output::Read;
    type BlockingReader: output::BlockingRead;
+    type Pager: output::Page;
+    type BlockingPager: output::BlockingPage;
}
```

## User defined layers

Due to this change, all layers implementation should be changed. If there is not changed over pager, they can by changed like the following:

```diff
impl<A: Accessor> LayeredAccessor for MyAccessor<A> {
    type Inner = A;
    type Reader = MyReader<A::Reader>;
    type BlockingReader = MyReader<A::BlockingReader>;
+    type Pager = A::Pager;
+    type BlockingPager = A::BlockingPager;

+    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
+        self.inner.list(path, args).await
+    }

+    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
+        self.inner.scan(path, args).await
+    }

+    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
+        self.inner.blocking_list(path, args)
+    }

+    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
+        self.inner.blocking_scan(path, args)
+    }
}
```

## Usage of ops

To reduce the understanding overhead, we move all `OpXxx` into `opendal::ops` now. User may need to change:

```diff
- use opendal::OpWrite;
+ use opendal::ops::OpWrite;
```

## Usage of RetryLayer

`backon` is the implementation detail of our `RetryLayer`, so we hide it from our public API. Users of `RetryLayer` need to change the code like:

```diff
- RetryLayer::new(backon::ExponentialBackoff::default())
+ RetryLayer::new()
```

# Upgrade to v0.26

In v0.26 we have replaced all internal dynamic dispatch usage with static dispatch. With this change, we can ensure that all operations performed inside OpenDAL are zero cost.

Due to this change, we have to refactor the logic of `Operator`'s init logic. In v0.26, we added `opendal::Builder` trait and `opendal::OperatorBuilder`. For the first glance, the only change to existing code will be like:

```diff
- let op = Operator::new(builder.build()?);
+ let op = Operator::new(builder.build()?).finish();
```

By adding a `finish()` call, we will erase all generic types so that `Operator` can still be easily to used everywhere as before.

## Accessor

In v0.26, `Accessor` has been changed into trait with associated types.

All services need to declare the types returned as `Reader` or `BlockingReader`:

```rust
pub trait Accessor: Send + Sync + Debug + Unpin + 'static {
    type Reader: output::Read;
    type BlockingReader: output::BlockingRead;
}
```

If your service doesn't support `read` or `blocking_read`, we can use `()` to represent an dummy reader:

```rust
impl Accessor for MyDummyAccessor {
    type Reader = ();
    type BlockingReader = ();
}
```

## Layer

As described before, OpenDAL prefer to use static dispatch. Layers are required to implement the new `Layer` and `LayeredAccessor` trait:

```rust
pub trait Layer<A: Accessor> {
    type LayeredAccessor: Accessor;

    fn layer(&self, inner: A) -> Self::LayeredAccessor;
}

#[async_trait]
pub trait LayeredAccessor: Send + Sync + Debug + Unpin + 'static {
    type Inner: Accessor;
    type Reader: output::Read;
    type BlockingReader: output::BlockingRead;
}
```

`LayeredAccessor` is a wrapper of `Accessor` with the typed `Innder`. All methods that not implemented will be forward to inner instead.

## Builder

Since v0.26, we implement `opendal::Builder` for all services, and services' mod will not be exported.

```diff
- use opendal::services::s3::Builder;
+ use opendal::services::S3;
```

## Conclusion

Sorry again for the big changes in this release. It's a big step for OpenDAL to work in more critical systems.

# Upgrade to v0.25

In v0.25, we bring the same feature sets from `ObjectReader` to `BlockingObjectReader`.

Due to this change, all code that depends on `BlockingBytesReader` should be refactored.

- `BlockingBytesReader` => `input::BlockingReader`
- `BlockingOutputBytesReader` => `output::BlockingReader`

Most changes only happen inside. Users not using `opendal::raw::*` will not be affected.

Apart from this change, we refactored s3 credential loading logic. After this change, we can disable the config load instead of the credential methods.

- `builder.disable_credential_loader` => `builder.disable_config_load`

# Upgrade to v0.24

In v0.24, we made a big refactor on our internal IO-related traits. In this version, we split our IO traits into `input` and `output` versions:

Take `Reader` as an example:

`input::Reader` is the user input reader, which only requires `futures::AsyncRead + Send`.

`output::Reader` is the reader returned by `OpenDAL`, which implements `futures::AsyncRead`, `futures::AsyncSeek`, and `futures::Stream<Item=io::Result<Bytes>>`. Besides, `output::Reader` also implements `Send + Sync`, which makes it useful for users.

Due to this change, all code that depends on `BytesReader` should be refactored.

- `BytesReader` => `input::Reader`
- `OutputBytesReader` => `output::Reader`

Thanks to the change of IO trait split, we make `ObjectReader` implements all needed traits:

- `futures::AsyncRead`
- `futures::AsyncSeek`
- `futures::Stream<Item=io::Result<Bytes>>`

Thus, we removed the `seekable_reader` API. They can be replaced by `range_reader`:

- `o.seekable_reader` => `o.range_reader`

Most changes only happen inside. Users not using `opendal::raw::*` will not be affected.

Sorry for the inconvenience. I think those changes are required and make OpenDAL better! Welcome any comments at [Discussion](https://github.com/apache/incubator-opendal/discussions).

# Upgrade to v0.21

v0.21 is an internal refactor version of OpenDAL. In this version, we refactored our error handling and our `Accessor` APIs. Thanks to those internal changes, we added an object-level metadata cache, making it nearly zero cost to reuse existing metadata continuously.

Let's start with our errors.

## Error Handling

As described in [RFC-0977: Refactor Error](https://opendal.apache.org/rfcs/0977-refactor-error.html), we refactor opendal error by a new error
called [`opendal::Error`](https://opendal.apache.org/opendal/struct.Error.html).

This change will affect all APIs that are used to return `io::Error`.

To migrate this, please replace `std::io::Error` with `opendal::Error`:

```diff
- use std::io::Result;
+ use opendal::Result;
```

And the following error kinds should be updated:

- `std::io::ErrorKind::NotFound` => `opendal::ErrorKind::ObjectNotFound`
- `std::io::ErrorKind::PermissionDenied` => `opendal::ErrorKind::ObjectPermissionDenied`

And since v0.21, we will return errors `ObjectIsADirectory` and `ObjectNotADirectory` instead of `anyhow::Error`.

## Accessor API

In v0.21, we refactor the whole `Accessor`'s API:

```diff
- async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64>
+ async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<RpWrite>
```

Since v0.21, we will return a reply struct for different operations called `RpWrite` instead of an exact type. We can split OpenDAL's public API and raw API with this change.

## ObjectList and Page

Since v0.21, `Accessor` will return `Pager` for `List`:

```diff
- async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer>
+ async fn list(&self, path: &str, args: OpList) -> Result<(RpList, output::Pager)>
```

And `Object` will return an `ObjectLister` which is built upon `Page`:

```rust
pub async fn list(&self) -> Result<ObjectLister> { ... }
```

`ObjectLister` can be used as an object stream as before. It also provides the function `next_page` to get the underlying pages directly:

```rust
impl ObjectLister {
    pub async fn next_page(&mut self) -> Result<Option<Vec<Object>>>;
}
```

## Code Layout

Since v0.21, we have categorized all APIs into `public` and `raw`.

Public APIs are exposed under `opendal::Xxx`; they are user-face APIs that are easy to use and understand.

Raw APIs are exposed under `opendal::raw::Xxx`; they are implementation details for underlying services and layers.

Please replace all usage of `opendal::io_util::*` and `opendal::http_util::*` to `opendal::raw::*` instead.

With this change, new users of OpenDAL maybe be it easier to get started.

## Summary

Sorry for introducing too much breaking change in a single version. This version can be a solid version for preparing OpenDAL v1.0.

# Upgrade to v0.20

v0.20 is a big release that we introduce a lot of performance related changes.

To make the best of information from `read` operation, we propose and implemented [RFC-0926: Object Reader](https://opendal.apache.org/rfcs/0926-object-reader.html). By this RFC, we can fetch content length from `ObjectReader` now!

```rust
pub struct ObjectReader {
    inner: BytesReader
    meta: ObjectMetadata,
}

impl ObjectReader {
    pub fn content_length(&self) -> u64 {}
    pub fn last_modified(&self) -> Option<OffsetDateTime> {}
    pub fn etag(&self) -> Option<String> {}
}
```

To make this happen, we changed our `Accessor` API:

```diff
- async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {}
+ async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {}
```

All layers should be updated to meet this change. Also, it's required to return `content_length` while building `ObjectReader`. Please make sure the returning `ObjectMetadata` is used correctly.

# Upgrade to v0.19

OpenDAL deprecate some features:

- `serde`: We will enable it by default.
- `layers-retry`: We will enable retry support by default.
- `layers-metadata-cache`: We will enable it by default.

Deprecated types like `DirEntry` has been removed.

# Upgrade to v0.18

OpenDAL v0.18 introduces the following breaking changes:

- Deprecated feature flag `services-http` has been removed.
- All `DirXxx` items have been renamed to `ObjectXxx` to make them more consistent.
  - `DirEntry` -> `Entry`
  - `DirStream` -> `ObjectStream`
  - `DirStreamer` -> `ObjectStream`
  - `DirIterate` -> `ObjectIterate`
  - `DirIterator` -> `ObjectIterator`

Besides, we also make a big change to our `Entry` API. Since v0.18, we can fully reuse the metadata that fetched during `list`. Take `entry.content_length()` for example:

- If `content_length` is already known, we will return directly.
- If not, we will check if the object entry is `complete`:
  - If `complete`, the entry already fetched all metadata that it could have, return directly.
  - If not, we will send a `stat` call to get the `metadata` and refresh our cache.

This change means:

- All API like `content_length` will be changed into async functions.
- `metadata` and `blocking_metadata` will not return errors anymore.
- To retrieve the latest meta, please use `entry.into_object().metadata()` instead.

# Upgrade to v0.17

OpenDAL v0.17 refactor the `Accessor` to make space for future features.

We move `path String` out of the `OpXxx` to function args so that we don't need to clone twice.

```diff
- async fn read(&self, args: OpRead) -> Result<BytesReader>
+ async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader>
```

For more information about this change, please refer to [RFC-0661: Path In Accessor](https://opendal.apache.org/rfcs/0661-path-in-accessor.html).

And since OpenDAL v0.17, we will use `rustls` as default tls engine for our underlying http client. Since this release, we will not depend on `openssl` anymore.

# Upgrade to v0.16

OpenDAL v0.16 refactor the internal implementation of `http` service. Since v0.16, http service can be used directly without enabling `services-http` feature. Accompany by these changes, http service has the following breaking changes:

- `services-http` feature has been deprecated. Enabling `services-http` is a no-op now.
- http service is read only services and can't be used to `list` or `write`.

OpenDAL introduces a new layer `ImmutableIndexLayer` that can add `list` capability for services:

```rust
use opendal::layers::ImmutableIndexLayer;
use opendal::Operator;
use opendal::Scheme;

async fn main() {
    let mut iil = ImmutableIndexLayer::default();

    for i in ["file", "dir/", "dir/file", "dir_without_prefix/file"] {
        iil.insert(i.to_string())
    }

    let op = Operator::from_env(Scheme::Http)?.layer(iil);
}
```

For more information about this change, please refer to [RFC-0627: Split Capabilities](https://opendal.apache.org/rfcs/0627-split-capabilities.html).

# Upgrade to v0.14

OpenDAL v0.14 removed all deprecated APIs in previous versions, including:

- `Operator::with_backoff` in v0.13
- All services `Builder::finish()` in v0.12
- All services `Backend::build()` in v0.12

Please visit related version's upgrade guide for migration.

And in OpenDAL v0.14, we introduce a break change for `write` operations.

```diff
pub trait Accessor {
    - async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {}
    + async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {}
}
```

The following APIs have affected by this change:

- `Object::write` now accept `impl Into<Vec<u8>>` instead of `AsRef<&[u8]>`
- `Object::writer` has been removed.
- `Object::write_from` has been added to support write from a reader.
- All layers should be refactored to adapt new `Accessor` trait.

For more information about this change, please refer to [RFC-0554: Write Refactor](https://opendal.apache.org/rfcs/0554-write-refactor.html).

# Upgrade to v0.13

OpenDAL deprecate `Operator::with_backoff` since v0.13.

Please use [`RetryLayer`](https://opendal.apache.org/opendal/layers/struct.RetryLayer.html) instead:

```rust
use anyhow::Result;
use backon::ExponentialBackoff;
use opendal::layers::RetryLayer;
use opendal::Operator;
use opendal::Scheme;

let _ = Operator::from_env(Scheme::Fs)
    .expect("must init")
    .layer(RetryLayer::new(ExponentialBackoff::default()));
```

# Upgrade to v0.12

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

For more information about this change, please refer to [RFC-0501: New Builder](https://opendal.apache.org/rfcs/0501-new-builder.html).

The following APIs have been deprecated:

- All services `Builder::finish()` (replaced by `Builder::build()`)
- All services `Backend::build()` (replace by `Builder::default()`)

The following APIs have been removed:

- public struct `Metadata` (deprecated in v0.8, replaced by `ObjectMetadata`)

# Upgrade to v0.8

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

# Upgrade to v0.7

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

# Upgrade to v0.4

OpenDAL introduces many breaking changes in v0.4.

## Object::reader() is not `AsyncSeek` anymore

Since v0.4, `Object::reader()` will return `impl BytesRead` instead of `Reader` that implements `AsyncRead` and `AsyncSeek`. Users who want `AsyncSeek` please wrapped with `opendal::io_util::seekable_read`:

```rust
use opendal::io_util::seekable_read;

let o = op.object("test");
let mut r = seekable_read(&o, 10..);
r.seek(SeekFrom::Current(10)).await?;
let mut bs = vec![0;10];
r.read(&mut bs).await?;
```

## Use RangeBounds instead

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

## Return io::Result instead

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

## Removing Credential

Since v0.4, `Credential` has been removed, please use the API provided by `Builder` directly.

```rust
builder.access_key_id("access_key_id");
builder.secret_access_key("secret_access_key");
```

## Write returns `BytesWriter` instead

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

## `io_util` replaces `readers`

Since v0.4, mod `io_util` will replace `readers`. In `io_utils`, OpenDAL provides helpful functions like:

- `into_reader`: Convert `BytesStream` into `BytesRead`
- `into_sink`: Convert `BytesWrite` into `BytesSink`
- `into_stream`: Convert `BytesRead` into `BytesStream`
- `into_writer`: Convert `BytesSink` into `BytesWrite`
- `observe_read`: Add callback for `BytesReader`
- `observe_write`: Add callback for `BytesWrite`

## New type alias

For better naming, types that OpenDAL returns have been renamed:

- `AsyncRead + Unpin + Send` => `BytesRead`
- `BoxedAsyncReader` => `BytesReader`
- `AsyncWrite + Unpin + Send` => `BytesWrite`
- `BoxedAsyncWriter` => `BytesWriter`
- `ObjectStream` => `ObjectStreamer`
