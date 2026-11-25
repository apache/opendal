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

`opendal_store_opendal` depends on the `opendal` crate. Please make sure to always use the latest versions of both.

latest `object_store_opendal` ![Crate](https://img.shields.io/crates/v/object_store_opendal.svg)

latest `opendal` ![Crate](https://img.shields.io/crates/v/opendal.svg)

### 1. using `object_store` API to access S3 

Add the following dependencies to your `Cargo.toml` with correct version:

```toml
[dependencies]
object_store = "0.14.1"
object_store_opendal =  "xxx"   # see the latest version above
opendal = { version = "xxx", features = ["services-s3"] }  # see the latest version above
```

Build `OpendalStore` via `opendal::Operator`:

```rust
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStoreExt;
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
    let operator = Operator::new(builder).unwrap();

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

### Path resolution

`OpendalStore` forwards every `object_store::path::Path` to the `Operator`,
which resolves it against the operator's configured `root`: a store path
`data/test.txt` reaches the service as `<root>/data/test.txt`. The store never
strips a leading prefix, because any prefix can be a legitimate object key
inside the root.

When the store is registered with a URL-based registry such as DataFusion's
`ObjectStoreRegistry`, the registry key contains only the registered URL's
scheme and authority, so the store receives the full path of each queried URL.
Keep the prefix in exactly one place to avoid the double-prefixing reported in
[apache/opendal#7786](https://github.com/apache/opendal/issues/7786):

- Leave the operator `root` unset so queried URLs carry the full path to each
  object, as `s3://bucket/key` does.
- Set `root` to the shared prefix and register the store under a URL whose
  queried paths are relative to it. For services such as `hf://` whose
  authority does not encode the bucket or repository, configure the repository
  on the builder and register a dedicated authority (for example
  `hf://myrepo`) so that `hf://myrepo/file` hands the store a
  repository-relative `file`.

### Object attributes and renames

`get` and `get_opts` expose the content attributes and user metadata returned
by the service in `GetResult.attributes`. To retrieve attributes without reading
the body, use `get_opts` with `GetOptions::with_head(true)`. The `head`
convenience method returns only `ObjectMeta`.

`rename` and `rename_if_not_exists` use OpenDAL's rename support when the
operator advertises the requested mode. Otherwise, the integration copies the
object and deletes the source after a successful copy. For distinct paths
(`from != to`), `rename_if_not_exists` requires conditional rename or
conditional copy, and `copy_if_not_exists` requires conditional copy, returning
`NotSupported` when the operator lacks those capabilities. A same-path overwrite
of an existing file succeeds and preserves its contents.

### Listing with an offset

`list_with_offset` returns objects whose encoded `object_store::path::Path`
compares greater than the offset. `OpendalStore` streams the requested prefix
from the backend and applies this comparison locally. With no prefix, the
backend listing starts at the operator root.

### Tune vectored reads

`OpendalStore` coalesces ranges separated by up to 1 MiB by default, matching
`object_store::OBJECT_STORE_COALESCE_DEFAULT`. Workloads that need to limit
over-fetching can configure the gap when constructing the store:

```rust
let object_store = OpendalStore::new(operator).with_get_ranges_gap(0);
```

A gap of `0` disables merging ranges separated by bytes. Overlapping or adjacent
ranges are still merged without over-fetching. Set an intermediate value to trade
fewer backend requests for additional bytes read. A gap of `0` can be useful for
HDFS scans that are sensitive to retaining large coalesced backing buffers.

### Tune multipart uploads

Set the buffered chunk size in bytes and the concurrency per multipart upload
when constructing the store:

```rust
let object_store = OpendalStore::new(operator)
    .with_multipart_chunk_size(16 * 1024 * 1024)
    .with_multipart_concurrency(4);
```

Both settings apply to multipart uploads, including `put_multipart` and
`put_multipart_opts`. They do not affect `put` or `put_opts`. Without an explicit
chunk size, OpenDAL continues to choose it from the service's capabilities. OpenDAL clamps an explicit size to the service's minimum and
maximum multipart sizes, and the final chunk may be smaller. The size must be
nonzero; passing zero panics.

Larger chunks reduce backend requests per byte uploaded, at the cost of more
buffered memory and more data to retransmit after a failed request.

Upload concurrency defaults to 8. Set it to 1 for sequential writes. Higher
concurrency can improve throughput at the cost of more buffered memory. Services
that do not support concurrent writes continue to write sequentially. Concurrency
must be nonzero; passing zero panics. This setting does not affect ordinary puts
or reads.

## Limitations and Backend-Specific Behavior

`OpendalStore` implements [`object_store::ObjectStore`] by delegating to an
`opendal::Operator`. Because OpenDAL targets many storage backends with
different native capabilities, some behaviors differ from native `object_store`
implementations (such as `InMemory`, `LocalFileSystem`, `AmazonS3`,
`GoogleCloudStorage`, or `MicrosoftAzure`):

- **Capability-dependent operations**: Optional operations depend on the
  underlying operator's capabilities (inspectable via
  `OpendalStore::info().capability()`) and return
  `object_store::Error::NotSupported` when unavailable:
  - `put_opts` with `PutMode::Create` requires `write_with_if_not_exists`.
  - `put_opts` with `PutMode::Update` requires at least one of `e_tag`
    (`write_with_if_match`) or `version` (`write_with_if_version_match`);
    supplying neither always returns `NotSupported`.
  - `copy` requires `copy`, and `copy_if_not_exists` requires
    `copy_with_if_not_exists`.
  - `rename` uses native `rename` when advertised, or falls back to
    `copy` followed by `delete`. The fallback is not atomic: if `delete` fails
    after `copy` succeeds, both source and destination remain.
  - `rename_if_not_exists` uses native `rename_with_if_not_exists` when
    advertised (for example, `fs` on Linux) or falls back to
    `copy_if_not_exists` followed by `delete`. That fallback only guards target
    creation and is not atomic with deleting the source; when neither
    capability is advertised, `rename_if_not_exists` returns `NotSupported`.
  - `get_opts` with `GetOptions::version` requires `read_with_version` for
    body reads, and `stat_with_version` for `GetOptions::head` or `stat`
    fallback paths (such as over-EOF ranges or local precondition evaluation).
- **Write atomicity and multipart uploads**:
  - Whether `put_opts`, `put_multipart_opts`, and `MultipartUpload::abort` are
    atomic depends on the underlying OpenDAL service. For example, OpenDAL's
    `fs` service writes directly to the target path by default and returns
    `NotSupported` from `abort()` unless configured with `atomic_write_dir`,
    whereas native `LocalFileSystem` always stages writes through temporary
    files.
  - `OpendalStore`'s `MultipartUpload` writes parts sequentially in call order
    through OpenDAL's `Writer` (which handles chunking and concurrent flushes
    internally). A failed, dropped, or cancelled `put_part` future poisons the
    upload so subsequent `put_part` and `complete` calls fail with
    `Error::Generic`; call `abort()` to clean up where supported. Await all
    `put_part` futures before calling `complete()`.
- **Metadata, attributes, tags, and extensions**:
  - `ObjectMeta::e_tag`, `ObjectMeta::version`, `PutResult::e_tag`, and
    `PutResult::version` are populated only when the backend returns them
    (`None` on backends such as `memory` and `fs`). When a backend omits
    `last_modified`, `ObjectMeta::last_modified` defaults to the Unix epoch
    (`1970-01-01T00:00:00Z`).
  - `OpendalStore` maps `CacheControl`, `ContentDisposition`,
    `ContentEncoding`, `ContentLanguage`, `ContentType`, and
    `Attribute::Metadata` on writes and reads. `Attribute::StorageClass` (and
    any future unknown `Attribute` variants) is ignored on writes and never
    returned on reads. OpenDAL's default correctness checks do not reject
    unsupported non-critical write attributes (such as `ContentType` on `fs`);
    layer `CapabilityCheckLayer` onto the `Operator` if strict rejection is
    needed.
  - `PutOptions::tags`, `PutMultipartOptions::tags`, and request `extensions`
    are ignored, and response `extensions` are always empty.
- **Conditional reads and payloads**:
  - When a service cannot satisfy `get_opts` preconditions or metadata directly
    on read open, `OpendalStore` falls back to `stat` plus local
    `GetOptions::check_preconditions` evaluation before opening the reader. On
    backends without native read preconditions, this two-step check is not
    atomic with reading the body, and `if_match` fails with
    `Error::Precondition` when the backend does not populate `e_tag`.
  - `OpendalStore` always returns `GetResultPayload::Stream`, never
    `GetResultPayload::File`.
- **Paths**:
  - `OpendalStore` percent-decodes an `object_store::path::Path` before passing
    it to OpenDAL. A path part that contains `/` (encoded as `%2F`) cannot be
    represented, because OpenDAL treats `/` as its separator. Operations on such
    paths return `Error::NotSupported` instead of addressing another object.
- **Deletes**:
  - OpenDAL's `delete` is idempotent: deleting a missing object succeeds rather
    than returning `Error::NotFound` (unlike native `LocalFileSystem`).
  - `delete_stream` deletes locations one by one sequentially through
    `Operator::delete` instead of using bulk-delete APIs or concurrent batches.
- **Listing**:
  - `list_with_offset` does not push `offset` down to backend cursors; it lists
    the requested prefix recursively and filters locally by encoded
    `object_store::path::Path` order, scanning entries before `offset`.
  - When a backend omits `last_modified` in list responses, `list` uses the
    Unix epoch. `list_with_offset` and `list_with_delimiter` issue an additional
    `stat` call for each returned object whose timestamp is missing.
  - Listing order follows the underlying backend and is not guaranteed to be
    sorted.
- **Optional `object_store` traits, error mapping, and `new_amazon_s3`**:
  - `OpendalStore` does not implement the optional `MultipartStore`, `Signer`,
    or `PaginatedListStore` traits from `object_store`.
  - OpenDAL maps `NotFound`, `Unsupported`, `AlreadyExists`,
    `ConditionNotMatch`, and `PermissionDenied` to dedicated `object_store::Error`
    variants; all other OpenDAL `ErrorKind` values map to `Error::Generic`.
  - `OpendalStore::new_amazon_s3` (with feature `services-s3`) translates only
    `Endpoint`, `Region`, `Bucket`, `AccessKeyId`, `SecretAccessKey`, `Token`,
    `VirtualHostedStyleRequest`, and `SkipSignature` from `AmazonS3Builder`;
    other S3 builder options are not forwarded.

[`object_store::ObjectStore`]: https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html

## WASM support

To build with `wasm32-unknown-unknown` target, you need to enable the `send_wrapper` feature:

```sh
cargo build --target wasm32-unknown-unknown --features send_wrapper
```

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
