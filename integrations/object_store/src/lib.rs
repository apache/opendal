// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! object_store_opendal is an object store implementation using opendal.
//!
//! This crate can help you to access 30 more storage services with the same object_store API.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use bytes::Bytes;
//! use object_store::path::Path;
//! use object_store::ObjectStore;
//! use object_store::ObjectStoreExt;
//! use object_store_opendal::OpendalStore;
//! use opendal::services::S3;
//! use opendal::{Builder, Operator};
//!
//! #[tokio::main]
//! async fn main() {
//!    let builder = S3::default()
//!     .access_key_id("my_access_key")
//!     .secret_access_key("my_secret_key")
//!     .endpoint("my_endpoint")
//!     .region("my_region");
//!
//!     // Create a new operator
//!     let operator = Operator::new(builder).unwrap();
//!
//!     // Create a new object store
//!     let object_store = Arc::new(OpendalStore::new(operator));
//!
//!     let path = Path::from("data/nested/test.txt");
//!     let bytes = Bytes::from_static(b"hello, world! I am nested.");
//!
//!     object_store.put(&path, bytes.clone().into()).await.unwrap();
//!
//!     let content = object_store
//!         .get(&path)
//!         .await
//!         .unwrap()
//!         .bytes()
//!         .await
//!         .unwrap();
//!
//!     assert_eq!(content, bytes);
//! }
//! ```
//!
//! # Path resolution
//!
//! `OpendalStore` forwards each [`object_store::path::Path`] to the `Operator`,
//! which resolves it against its configured `root`; prefixes are never
//! stripped. When the store is registered with a URL-based registry such as
//! DataFusion's `ObjectStoreRegistry`, keep the shared prefix in exactly one
//! place — in the queried URL paths or in the operator `root` — to avoid
//! double-prefixing. See [`OpendalStore::new`] for the full convention and a
//! runnable example.
//!
//! Use the re-exported future helper when an integration needs the same OpenDAL
//! future wrapper used by this crate:
//!
//! ```no_run
//! use object_store_opendal::IntoSendFuture;
//! use opendal::Operator;
//!
//! async fn rename(op: Operator) -> opendal::Result<()> {
//!     op.rename("from", "to").into_send().await
//! }
//! ```
//!
//! # Limitations and Backend-Specific Behavior
//!
//! [`OpendalStore`] implements [`object_store::ObjectStore`] by delegating to
//! an [`opendal::Operator`]. Because OpenDAL supports many storage backends
//! with different native capabilities, some behaviors differ from native
//! `object_store` implementations:
//!
//! - **Capability-dependent operations**: Optional operations depend on
//!   [`OpendalStore::info`]'s capability flags and return
//!   [`object_store::Error::NotSupported`] when unavailable:
//!   - [`put_opts`](object_store::ObjectStore::put_opts) with
//!     [`PutMode::Create`](object_store::PutMode::Create) requires
//!     `write_with_if_not_exists`.
//!   - [`put_opts`](object_store::ObjectStore::put_opts) with
//!     [`PutMode::Update`](object_store::PutMode::Update) requires at least one
//!     of `e_tag` (`write_with_if_match`) or `version`
//!     (`write_with_if_version_match`); supplying neither always returns
//!     `NotSupported`.
//!   - [`copy_opts`](object_store::ObjectStore::copy_opts) requires `copy` for
//!     overwrite and `copy_with_if_not_exists` for create.
//!   - [`rename_opts`](object_store::ObjectStore::rename_opts) uses native
//!     `rename` or `rename_with_if_not_exists` when advertised, and otherwise
//!     falls back to `copy` (or `copy_if_not_exists`) followed by `delete`.
//!     That fallback is not atomic with deleting the source, and create-mode
//!     rename returns `NotSupported` when the operator supports neither
//!     `rename_with_if_not_exists` nor `copy_with_if_not_exists`.
//!   - [`get_opts`](object_store::ObjectStore::get_opts) with
//!     [`GetOptions::version`](object_store::GetOptions::version) requires
//!     `read_with_version` for body reads, and `stat_with_version` for
//!     [`GetOptions::head`](object_store::GetOptions::head) or `stat` fallback
//!     paths (such as over-EOF ranges or local precondition evaluation).
//! - **Write atomicity and multipart uploads**:
//!   - Write atomicity and [`MultipartUpload::abort`](object_store::MultipartUpload::abort)
//!     depend on the underlying OpenDAL service. For example, OpenDAL's `fs`
//!     service writes directly to the target path by default and returns
//!     `NotSupported` from `abort` unless configured with `atomic_write_dir`.
//!   - Multipart uploads write parts sequentially in call order through
//!     OpenDAL's `Writer` (which buffers chunks and flushes concurrently
//!     internally). A failed, dropped, or cancelled `put_part` future poisons
//!     the upload so subsequent `put_part` and `complete` calls fail with
//!     [`object_store::Error::Generic`]; call `abort` to clean up where
//!     supported. Calling `complete` with parts still in flight also fails and
//!     poisons the upload.
//! - **Metadata, attributes, tags, and extensions**:
//!   - `e_tag` and `version` on [`object_store::ObjectMeta`] and
//!     [`object_store::PutResult`] are populated only when the backend returns
//!     them (`None` on backends such as `memory` and `fs`). When a backend omits
//!     `last_modified`, [`ObjectMeta::last_modified`](object_store::ObjectMeta::last_modified)
//!     defaults to the Unix epoch (`1970-01-01T00:00:00Z`).
//!   - [`OpendalStore`] maps `CacheControl`, `ContentDisposition`,
//!     `ContentEncoding`, `ContentLanguage`, `ContentType`, and
//!     [`Attribute::Metadata`](object_store::Attribute::Metadata).
//!     [`Attribute::StorageClass`](object_store::Attribute::StorageClass) (and
//!     unknown future variants) is ignored on writes and never populated on
//!     reads. OpenDAL's default correctness checks do not reject unsupported
//!     non-critical write attributes; add `CapabilityCheckLayer` to the
//!     `Operator` when strict rejection is required.
//!   - [`PutOptions::tags`](object_store::PutOptions::tags),
//!     [`PutMultipartOptions::tags`](object_store::PutMultipartOptions::tags),
//!     and request `extensions` are ignored; response `extensions` are always
//!     empty.
//! - **Conditional reads and payloads**:
//!   - When a service cannot return metadata or evaluate preconditions directly
//!     on read open, [`get_opts`](object_store::ObjectStore::get_opts) falls
//!     back to `stat` plus local precondition checks before opening the reader.
//!     On backends without native read preconditions, this check is not atomic
//!     with reading the body, and `if_match` fails with
//!     [`object_store::Error::Precondition`] when the backend omits `e_tag`.
//!   - [`OpendalStore`] always returns
//!     [`GetResultPayload::Stream`](object_store::GetResultPayload::Stream),
//!     never [`GetResultPayload::File`](object_store::GetResultPayload::File).
//! - **Deletes**:
//!   - Deleting a missing object succeeds idempotently (matching OpenDAL's
//!     delete contract), rather than returning [`object_store::Error::NotFound`].
//!   - [`delete_stream`](object_store::ObjectStore::delete_stream) deletes
//!     objects sequentially one by one through [`opendal::Operator::delete`]
//!     instead of issuing bulk or concurrent delete batches.
//! - **Listing**:
//!   - [`list_with_offset`](object_store::ObjectStore::list_with_offset) lists
//!     the full prefix recursively and filters locally by encoded
//!     [`object_store::path::Path`] order instead of pushing a cursor down to
//!     the backend.
//!   - When list entries omit `last_modified`, `list`, `list_with_offset`, and
//!     `list_with_delimiter` issue an extra `stat` call per object to complete
//!     [`object_store::ObjectMeta`].
//!   - Listing order is backend-defined and not guaranteed to be sorted.
//! - **Optional `object_store` traits, error mapping, and `new_amazon_s3`**:
//!   - [`OpendalStore`] does not implement [`object_store::multipart::MultipartStore`],
//!     [`object_store::signer::Signer`], or
//!     [`object_store::list::PaginatedListStore`].
//!   - OpenDAL maps `NotFound`, `Unsupported`, `AlreadyExists`,
//!     `ConditionNotMatch`, and `PermissionDenied` to dedicated
//!     [`object_store::Error`] variants; all other [`opendal::ErrorKind`] values
//!     map to [`object_store::Error::Generic`].
//!   - With feature `services-s3`, `OpendalStore::new_amazon_s3` forwards only
//!     `Endpoint`, `Region`, `Bucket`, `AccessKeyId`, `SecretAccessKey`,
//!     `Token`, `VirtualHostedStyleRequest`, and `SkipSignature` from
//!     `AmazonS3Builder`; other S3 builder options are not forwarded.

mod store;
pub use store::OpendalStore;

mod utils;
pub use utils::IntoSendFuture;

#[cfg(feature = "services-s3")]
mod amazon_s3;

mod service;

pub use service::{ObjectStoreBuilder, ObjectStoreService};

// Make sure `send_wrapper` works as expected
#[cfg(all(feature = "send_wrapper", test))]
mod assert_send {
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
    use opendal::Operator;

    #[allow(dead_code)]
    fn assert_send<T: Send>(_: T) {}

    #[allow(dead_code)]
    fn assertion() {
        let op = Operator::new(opendal::services::Memory::default()).unwrap();
        let store = super::OpendalStore::new(op);
        assert_send(store.put(&"test".into(), PutPayload::new()));
        assert_send(store.get(&"test".into()));
        assert_send(store.get_range(&"test".into(), 0..1));
        assert_send(store.head(&"test".into()));
        assert_send(store.delete(&"test".into()));
        assert_send(store.list(None));
        assert_send(store.list_with_offset(None, &"test".into()));
        assert_send(store.list_with_delimiter(None));
    }
}

fn timestamp_to_datetime(ts: opendal::raw::Timestamp) -> Option<chrono::DateTime<chrono::Utc>> {
    let jiff_ts = ts.into_inner();
    // `jiff` truncates `as_second` toward zero and reports a signed subsecond,
    // so pre-epoch timestamps can carry a negative nanosecond. `chrono` expects
    // a non-negative nanosecond component, borrowed from the preceding second.
    let mut second = jiff_ts.as_second();
    let mut nanosecond = jiff_ts.subsec_nanosecond();
    if nanosecond < 0 {
        second = second.checked_sub(1)?;
        nanosecond += 1_000_000_000;
    }
    chrono::DateTime::<chrono::Utc>::from_timestamp(second, nanosecond as u32)
}

fn datetime_to_timestamp(dt: chrono::DateTime<chrono::Utc>) -> Option<opendal::raw::Timestamp> {
    opendal::raw::Timestamp::new(dt.timestamp(), dt.timestamp_subsec_nanos() as i32).ok()
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;

    fn timestamp(second: i64, nanosecond: i32) -> opendal::raw::Timestamp {
        opendal::raw::Timestamp::new(second, nanosecond).expect("timestamp should be valid")
    }

    #[test]
    fn timestamp_to_datetime_handles_negative_subseconds() {
        for (second, nanosecond) in [
            (0, -999_999_999),
            (0, -500_000_000),
            (-1, -999_999_999),
            (-1, -500_000_000),
            (-2, -1),
            (0, 0),
            (-1, 0),
            (0, 500_000_000),
            (1, 999_999_999),
        ] {
            let total_nanoseconds = i128::from(second) * 1_000_000_000 + i128::from(nanosecond);
            let expected = chrono::DateTime::<chrono::Utc>::from_timestamp(
                total_nanoseconds.div_euclid(1_000_000_000) as i64,
                total_nanoseconds.rem_euclid(1_000_000_000) as u32,
            )
            .expect("expected datetime should be valid");

            let actual = timestamp_to_datetime(timestamp(second, nanosecond))
                .unwrap_or_else(|| panic!("({second}, {nanosecond}) should convert"));
            assert_eq!(actual, expected, "({second}, {nanosecond})");
        }
    }

    #[test]
    fn timestamp_to_datetime_handles_supported_range() {
        for ts in [opendal::raw::Timestamp::MIN, opendal::raw::Timestamp::MAX] {
            assert!(timestamp_to_datetime(ts).is_some());
        }
    }
}
