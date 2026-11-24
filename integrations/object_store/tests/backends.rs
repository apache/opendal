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

//! Multi-backend integration tests for [`OpendalStore`].
//!
//! Exercises the [`ObjectStore`] trait across locally available OpenDAL
//! backends with distinct capability profiles:
//! - `memory`: in-memory backend with `write_with_if_not_exists` and content/user attributes,
//!   without ETags, versioning, copy, or rename.
//! - `fs`: POSIX filesystem backend (default config without `atomic_write_dir`) with native
//!   `copy`, `rename`, `rename_with_if_not_exists` (Linux), `write_with_if_not_exists`, and date
//!   preconditions, without ETags, `copy_with_if_not_exists`, or multipart `abort`.
//! - `fs_atomic`: POSIX filesystem backend configured with `atomic_write_dir`, enabling atomic
//!   writes and `MultipartUpload::abort`.
//! - `object_store_in_memory`: OpenDAL `ObjectStoreBuilder` over `object_store::memory::InMemory`,
//!   providing ETags and `stat_with_if_match`, without `write_with_if_not_exists`, `copy`, or `rename`.
//! - `object_store_local_fs`: OpenDAL `ObjectStoreBuilder` over `object_store::local::LocalFileSystem`,
//!   providing filesystem ETags and rejecting attributes, without `write_with_if_not_exists`, `copy`, or `rename`.
//! - `versioned_copy_memory`: S3-like capability profile layered over `memory`, providing
//!   ETags, versions, conditional writes (`if_match`, `if_version_match`), versioned reads,
//!   `copy`, and `copy_with_if_not_exists`, without native `rename`.

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, UpdateVersion, WriteMultipart,
};
use object_store_opendal::{ObjectStoreBuilder, OpendalStore};
use opendal::layers::CapabilityOverrideLayer;
use opendal::raw::oio::{self, Write as _};
use opendal::raw::{
    Layer, OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    RpCreateDir, RpPresign, RpRead, RpRename, RpStat, Service, ServiceInfo, Servicer, Timestamp,
};
use opendal::{Buffer, BytesRange, Capability, Error, ErrorKind, MetadataBuilder, Operator};
use tempfile::TempDir;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn with_timeout<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .expect("integration test timed out")
}

struct TestBackend {
    name: &'static str,
    store: OpendalStore,
    /// Whether the backend persists HTTP content and user-metadata attributes.
    persists_attributes: bool,
    /// Whether the backend rejects non-empty attributes with `NotSupported`.
    rejects_attributes: bool,
    /// Whether the backend populates `ObjectMeta::e_tag`.
    provides_etag: bool,
    /// Whether `MultipartUpload::abort` is supported after writing a part.
    supports_multipart_abort: bool,
    _tempdir: Option<TempDir>,
}

fn build_backends() -> Vec<TestBackend> {
    let mem_op = Operator::new(opendal::services::Memory::default()).unwrap();

    let fs_dir = TempDir::new().unwrap();
    let fs_op =
        Operator::new(opendal::services::Fs::default().root(fs_dir.path().to_str().unwrap()))
            .unwrap();

    let fs_atomic_dir = TempDir::new().unwrap();
    let fs_atomic_root = fs_atomic_dir.path().join("root");
    let fs_atomic_staging = fs_atomic_dir.path().join("staging");
    std::fs::create_dir_all(&fs_atomic_root).unwrap();
    std::fs::create_dir_all(&fs_atomic_staging).unwrap();
    let fs_atomic_op = Operator::new(
        opendal::services::Fs::default()
            .root(fs_atomic_root.to_str().unwrap())
            .atomic_write_dir(fs_atomic_staging.to_str().unwrap()),
    )
    .unwrap();

    let os_mem_op = Operator::new(ObjectStoreBuilder::new(Arc::new(InMemory::new()))).unwrap();

    let os_fs_dir = TempDir::new().unwrap();
    let local_fs = LocalFileSystem::new_with_prefix(os_fs_dir.path()).unwrap();
    let os_fs_op = Operator::new(ObjectStoreBuilder::new(Arc::new(local_fs))).unwrap();

    let versioned_op = Operator::new(opendal::services::Memory::default())
        .unwrap()
        .layer(versioned_copy_layer::VersionedCopyLayer);

    vec![
        TestBackend {
            name: "memory",
            store: OpendalStore::new(mem_op),
            persists_attributes: true,
            rejects_attributes: false,
            provides_etag: false,
            supports_multipart_abort: true,
            _tempdir: None,
        },
        TestBackend {
            name: "fs",
            store: OpendalStore::new(fs_op),
            persists_attributes: false,
            rejects_attributes: false,
            provides_etag: false,
            supports_multipart_abort: false,
            _tempdir: Some(fs_dir),
        },
        TestBackend {
            name: "fs_atomic",
            store: OpendalStore::new(fs_atomic_op),
            persists_attributes: false,
            rejects_attributes: false,
            provides_etag: false,
            supports_multipart_abort: true,
            _tempdir: Some(fs_atomic_dir),
        },
        TestBackend {
            name: "object_store_in_memory",
            store: OpendalStore::new(os_mem_op),
            persists_attributes: false,
            rejects_attributes: false,
            provides_etag: true,
            supports_multipart_abort: true,
            _tempdir: None,
        },
        TestBackend {
            name: "object_store_local_fs",
            store: OpendalStore::new(os_fs_op),
            persists_attributes: false,
            rejects_attributes: true,
            provides_etag: true,
            supports_multipart_abort: true,
            _tempdir: Some(os_fs_dir),
        },
        TestBackend {
            name: "versioned_copy_memory",
            store: OpendalStore::new(versioned_op),
            persists_attributes: true,
            rejects_attributes: false,
            provides_etag: true,
            supports_multipart_abort: true,
            _tempdir: None,
        },
    ]
}

#[tokio::test]
async fn test_put_opts_modes_across_backends() {
    with_timeout(async {
        for backend in build_backends() {
            let store = &backend.store;
            let cap = store.info().capability();
            let name = backend.name;

            // PutMode::Overwrite creates and replaces an object.
            let path = Path::from("put/overwrite.txt");
            let first = store
                .put_opts(
                    &path,
                    Bytes::from_static(b"v1").into(),
                    PutOptions {
                        mode: PutMode::Overwrite,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_or_else(|e| panic!("[{name}] overwrite create failed: {e}"));
            if backend.provides_etag {
                assert!(
                    first.e_tag.is_some(),
                    "[{name}] expected PutResult.e_tag on backend with ETags"
                );
            }

            let second = store
                .put_opts(
                    &path,
                    Bytes::from_static(b"v2").into(),
                    PutOptions {
                        mode: PutMode::Overwrite,
                        ..Default::default()
                    },
                )
                .await
                .unwrap_or_else(|e| panic!("[{name}] overwrite replace failed: {e}"));
            assert_eq!(
                store.get(&path).await.unwrap().bytes().await.unwrap(),
                Bytes::from_static(b"v2"),
                "[{name}] overwrite body mismatch"
            );
            if backend.provides_etag {
                assert!(second.e_tag.is_some(), "[{name}] expected updated e_tag");
            }

            // PutMode::Create obeys write_with_if_not_exists capability.
            let create_path = Path::from("put/create.txt");
            let create_opts = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            if cap.write_with_if_not_exists {
                store
                    .put_opts(
                        &create_path,
                        Bytes::from_static(b"created").into(),
                        create_opts.clone(),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] create on absent path failed: {e}"));
                let err = store
                    .put_opts(
                        &create_path,
                        Bytes::from_static(b"duplicate").into(),
                        create_opts,
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, object_store::Error::AlreadyExists { ref path, .. } if path == create_path.as_ref()),
                    "[{name}] expected AlreadyExists on duplicate Create, got {err:?}"
                );
                assert_eq!(
                    store.get(&create_path).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"created"),
                    "[{name}] duplicate Create must not overwrite"
                );
            } else {
                let err = store
                    .put_opts(
                        &create_path,
                        Bytes::from_static(b"created").into(),
                        create_opts,
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for PutMode::Create, got {err:?}"
                );
            }

            // PutMode::Update with neither e_tag nor version is always rejected.
            let empty_update = PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: None,
                    version: None,
                }),
                ..Default::default()
            };
            let err = store
                .put_opts(&path, Bytes::from_static(b"v3").into(), empty_update)
                .await
                .unwrap_err();
            assert!(
                matches!(err, object_store::Error::NotSupported { .. }),
                "[{name}] expected NotSupported for empty UpdateVersion, got {err:?}"
            );

            // PutMode::Update with e_tag obeys write_with_if_match.
            let meta = store.head(&path).await.unwrap();
            let etag_update = PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: Some(meta.e_tag.clone().unwrap_or_else(|| "dummy-etag".into())),
                    version: None,
                }),
                ..Default::default()
            };
            if cap.write_with_if_match {
                let updated = store
                    .put_opts(&path, Bytes::from_static(b"v3").into(), etag_update)
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] matching if_match update failed: {e}"));
                assert_ne!(updated.e_tag, meta.e_tag, "[{name}] expected new etag after update");
                assert_eq!(
                    store.get(&path).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"v3")
                );

                let stale_err = store
                    .put_opts(
                        &path,
                        Bytes::from_static(b"stale").into(),
                        PutOptions {
                            mode: PutMode::Update(UpdateVersion {
                                e_tag: meta.e_tag.clone(),
                                version: None,
                            }),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(stale_err, object_store::Error::Precondition { .. }),
                    "[{name}] expected Precondition for stale etag update, got {stale_err:?}"
                );
            } else {
                let err = store
                    .put_opts(&path, Bytes::from_static(b"v3").into(), etag_update)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for PutMode::Update(e_tag), got {err:?}"
                );
            }

            // PutMode::Update with version obeys write_with_if_version_match.
            let current_meta = store.head(&path).await.unwrap();
            let version_update = PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: None,
                    version: Some(
                        current_meta
                            .version
                            .clone()
                            .unwrap_or_else(|| "dummy-version".into()),
                    ),
                }),
                ..Default::default()
            };
            if cap.write_with_if_version_match {
                let updated = store
                    .put_opts(&path, Bytes::from_static(b"v4").into(), version_update)
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] matching version update failed: {e}"));
                assert_ne!(updated.version, current_meta.version);
                assert_eq!(
                    store.get(&path).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"v4")
                );

                let stale_err = store
                    .put_opts(
                        &path,
                        Bytes::from_static(b"stale-ver").into(),
                        PutOptions {
                            mode: PutMode::Update(UpdateVersion {
                                e_tag: None,
                                version: current_meta.version.clone(),
                            }),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(stale_err, object_store::Error::Precondition { .. }),
                    "[{name}] expected Precondition for stale version update, got {stale_err:?}"
                );
            } else {
                let err = store
                    .put_opts(&path, Bytes::from_static(b"v4").into(), version_update)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for PutMode::Update(version), got {err:?}"
                );
            }
        }
    })
    .await;
}

#[tokio::test]
async fn test_put_multipart_opts_across_backends() {
    with_timeout(async {
        for backend in build_backends() {
            let store = &backend.store;
            let name = backend.name;

            // Multi-part upload with explicit put_part + complete.
            let path = Path::from("multipart/manual.bin");
            let mut upload = store
                .put_multipart_opts(&path, PutMultipartOptions::default())
                .await
                .unwrap_or_else(|e| panic!("[{name}] put_multipart_opts failed: {e}"));
            upload
                .put_part(Bytes::from_static(b"hello ").into())
                .await
                .unwrap_or_else(|e| panic!("[{name}] put_part 1 failed: {e}"));
            upload
                .put_part(Bytes::from_static(b"multipart ").into())
                .await
                .unwrap_or_else(|e| panic!("[{name}] put_part 2 failed: {e}"));
            upload
                .put_part(Bytes::from_static(b"world").into())
                .await
                .unwrap_or_else(|e| panic!("[{name}] put_part 3 failed: {e}"));
            let put_res = upload
                .complete()
                .await
                .unwrap_or_else(|e| panic!("[{name}] complete failed: {e}"));
            if backend.provides_etag {
                assert!(put_res.e_tag.is_some(), "[{name}] expected multipart e_tag");
            }
            let expected = Bytes::from_static(b"hello multipart world");
            assert_eq!(store.head(&path).await.unwrap().size, expected.len() as u64);
            assert_eq!(
                store.get(&path).await.unwrap().bytes().await.unwrap(),
                expected,
                "[{name}] multipart content mismatch"
            );

            // WriteMultipart helper with chunked payload.
            let helper_path = Path::from("multipart/write_helper.bin");
            let upload = store.put_multipart(&helper_path).await.unwrap();
            let mut writer = WriteMultipart::new(upload);
            let payload = vec![b'x'; 4096];
            writer.write(&payload[..1500]);
            writer.write(&payload[1500..3000]);
            writer.write(&payload[3000..]);
            writer
                .finish()
                .await
                .unwrap_or_else(|e| panic!("[{name}] WriteMultipart finish failed: {e}"));
            assert_eq!(
                store
                    .get(&helper_path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                Bytes::from(payload),
                "[{name}] WriteMultipart content mismatch"
            );

            // Abort discards staged parts when supported; services without
            // staging (such as `fs` without `atomic_write_dir`) report NotSupported.
            let aborted_path = Path::from("multipart/aborted.bin");
            let mut upload = store.put_multipart(&aborted_path).await.unwrap();
            upload
                .put_part(Bytes::from_static(b"temporary").into())
                .await
                .unwrap();
            let abort_res = upload.abort().await;
            if backend.supports_multipart_abort {
                abort_res.unwrap_or_else(|e| panic!("[{name}] abort failed: {e}"));
                let err = store.head(&aborted_path).await.unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotFound { .. }),
                    "[{name}] aborted upload must not be visible, got {err:?}"
                );
            } else {
                let err = abort_res.unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported on abort without staging, got {err:?}"
                );
            }

            // put_multipart_opts forwards attributes when supported.
            let attr_path = Path::from("multipart/with_attrs.txt");
            let attrs = Attributes::from_iter([
                (Attribute::CacheControl, "no-cache"),
                (Attribute::ContentType, "application/octet-stream"),
                (Attribute::Metadata("part_owner".into()), "opendal"),
            ]);
            let opts = PutMultipartOptions {
                attributes: attrs.clone(),
                ..Default::default()
            };
            let mut upload = store
                .put_multipart_opts(&attr_path, opts)
                .await
                .unwrap_or_else(|e| panic!("[{name}] put_multipart_opts(attrs) failed: {e}"));
            let write_and_complete = async {
                upload
                    .put_part(Bytes::from_static(b"part-a").into())
                    .await?;
                upload
                    .put_part(Bytes::from_static(b"part-b").into())
                    .await?;
                upload.complete().await
            }
            .await;
            if backend.rejects_attributes {
                let err = write_and_complete.unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for multipart attributes, got {err:?}"
                );
            } else {
                write_and_complete
                    .unwrap_or_else(|e| panic!("[{name}] multipart with attrs failed: {e}"));
                let got = store.get(&attr_path).await.unwrap();
                if backend.persists_attributes {
                    assert_eq!(
                        got.attributes, attrs,
                        "[{name}] multipart attributes mismatch"
                    );
                }
                assert_eq!(
                    got.bytes().await.unwrap(),
                    Bytes::from_static(b"part-apart-b")
                );
            }
        }
    })
    .await;
}

#[tokio::test]
async fn test_get_opts_ranges_conditionals_versions_and_attributes_across_backends() {
    with_timeout(async {
        for backend in build_backends() {
            let store = &backend.store;
            let cap = store.info().capability();
            let name = backend.name;

            let path = Path::from("get/sample.txt");
            let body = Bytes::from_static(b"0123456789abcdef");
            let first_put = store.put(&path, body.clone().into()).await.unwrap();
            let meta = store.head(&path).await.unwrap();
            assert_eq!(meta.location, path);
            assert_eq!(meta.size, body.len() as u64);

            // Range reads: Bounded, Offset, Suffix, over-EOF clamp, and get_ranges.
            let bounded = store
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Bounded(4..10))),
                )
                .await
                .unwrap();
            assert_eq!(bounded.range, 4..10);
            assert_eq!(bounded.meta.size, body.len() as u64);
            assert_eq!(bounded.bytes().await.unwrap(), Bytes::from_static(b"456789"));

            let offset = store
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Offset(12))),
                )
                .await
                .unwrap();
            assert_eq!(offset.range, 12..16);
            assert_eq!(offset.bytes().await.unwrap(), Bytes::from_static(b"cdef"));

            let suffix = store
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Suffix(5))),
                )
                .await
                .unwrap();
            assert_eq!(suffix.range, 11..16);
            assert_eq!(suffix.bytes().await.unwrap(), Bytes::from_static(b"bcdef"));

            let clamped = store
                .get_opts(
                    &path,
                    GetOptions::new().with_range(Some(GetRange::Bounded(12..100))),
                )
                .await
                .unwrap();
            assert_eq!(clamped.range, 12..16);
            assert_eq!(clamped.bytes().await.unwrap(), Bytes::from_static(b"cdef"));

            let ranges = store
                .get_ranges(&path, &[0..4, 6..10, 14..30])
                .await
                .unwrap_or_else(|e| panic!("[{name}] get_ranges failed: {e}"));
            assert_eq!(
                ranges,
                vec![
                    Bytes::from_static(b"0123"),
                    Bytes::from_static(b"6789"),
                    Bytes::from_static(b"ef"),
                ],
                "[{name}] get_ranges mismatch"
            );

            // head: true returns metadata with an empty payload stream.
            let head_opts = store
                .get_opts(&path, GetOptions::new().with_head(true))
                .await
                .unwrap();
            assert_eq!(head_opts.range, 0..0);
            assert_eq!(head_opts.meta.size, body.len() as u64);
            assert!(head_opts.bytes().await.unwrap().is_empty());

            // Date preconditions work across all backends (natively or via stat fallback).
            let earlier = meta.last_modified - chrono::Duration::seconds(10);
            let later = meta.last_modified + chrono::Duration::seconds(10);
            let ok_mod = store
                .get_opts(&path, GetOptions::new().with_if_modified_since(Some(earlier)))
                .await
                .unwrap_or_else(|e| panic!("[{name}] if_modified_since(earlier) failed: {e}"));
            assert_eq!(ok_mod.bytes().await.unwrap(), body);

            let err_not_mod = store
                .get_opts(&path, GetOptions::new().with_if_modified_since(Some(later)))
                .await
                .unwrap_err();
            assert!(
                matches!(err_not_mod, object_store::Error::NotModified { .. }),
                "[{name}] expected NotModified for if_modified_since(later), got {err_not_mod:?}"
            );

            let ok_unmod = store
                .get_opts(
                    &path,
                    GetOptions::new().with_if_unmodified_since(Some(later)),
                )
                .await
                .unwrap_or_else(|e| panic!("[{name}] if_unmodified_since(later) failed: {e}"));
            assert_eq!(ok_unmod.bytes().await.unwrap(), body);

            let err_precond = store
                .get_opts(
                    &path,
                    GetOptions::new().with_if_unmodified_since(Some(earlier)),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err_precond, object_store::Error::Precondition { .. }),
                "[{name}] expected Precondition for if_unmodified_since(earlier), got {err_precond:?}"
            );

            // ETag preconditions depend on whether the backend populates meta.e_tag.
            if let Some(etag) = meta.e_tag.clone() {
                let matched = store
                    .get_opts(&path, GetOptions::new().with_if_match(Some(etag.clone())))
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] matching if_match failed: {e}"));
                assert_eq!(matched.bytes().await.unwrap(), body);

                let mismatch = store
                    .get_opts(
                        &path,
                        GetOptions::new().with_if_match(Some("wrong-etag".to_string())),
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(mismatch, object_store::Error::Precondition { .. }),
                    "[{name}] expected Precondition for wrong if_match, got {mismatch:?}"
                );

                let not_mod = store
                    .get_opts(&path, GetOptions::new().with_if_none_match(Some(etag)))
                    .await
                    .unwrap_err();
                assert!(
                    matches!(not_mod, object_store::Error::NotModified { .. }),
                    "[{name}] expected NotModified for matching if_none_match, got {not_mod:?}"
                );

                let diff_none_match = store
                    .get_opts(
                        &path,
                        GetOptions::new().with_if_none_match(Some("other-etag".to_string())),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] non-matching if_none_match failed: {e}"));
                assert_eq!(diff_none_match.bytes().await.unwrap(), body);
            } else {
                // Without ETags, local precondition evaluation fails if_match for a concrete tag.
                let mismatch = store
                    .get_opts(
                        &path,
                        GetOptions::new().with_if_match(Some("any-etag".to_string())),
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(mismatch, object_store::Error::Precondition { .. }),
                    "[{name}] expected Precondition for if_match without etag, got {mismatch:?}"
                );
            }

            // GetOptions::version obeys read_with_version for body reads and
            // stat_with_version for head requests.
            let v1 = if cap.read_with_version || cap.stat_with_version {
                let v1 = first_put
                    .version
                    .clone()
                    .expect("versioned backend must return version");
                store
                    .put(&path, Bytes::from_static(b"updated-v2").into())
                    .await
                    .unwrap();
                Some(v1)
            } else {
                None
            };
            let requested_version = v1.clone().unwrap_or_else(|| "v1".to_string());

            if cap.read_with_version {
                let old = store
                    .get_opts(
                        &path,
                        GetOptions::new().with_version(Some(requested_version.clone())),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] versioned get failed: {e}"));
                assert_eq!(old.bytes().await.unwrap(), body);
            } else {
                let err = store
                    .get_opts(
                        &path,
                        GetOptions::new().with_version(Some(requested_version.clone())),
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for versioned get, got {err:?}"
                );
            }

            if cap.stat_with_version {
                let old_head = store
                    .get_opts(
                        &path,
                        GetOptions::new()
                            .with_head(true)
                            .with_version(Some(requested_version)),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] versioned head failed: {e}"));
                assert_eq!(old_head.range, 0..0);
                assert_eq!(old_head.meta.size, body.len() as u64);
                assert_eq!(old_head.meta.version, v1);
                assert!(old_head.bytes().await.unwrap().is_empty());
            } else {
                let err = store
                    .get_opts(
                        &path,
                        GetOptions::new()
                            .with_head(true)
                            .with_version(Some(requested_version)),
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for versioned head, got {err:?}"
                );
            }

            // Attributes round-trip or rejection depending on backend capabilities.
            let attr_path = Path::from("get/attributes.txt");
            let attrs = Attributes::from_iter([
                (Attribute::CacheControl, "max-age=60"),
                (Attribute::ContentDisposition, "inline"),
                (Attribute::ContentEncoding, "identity"),
                (Attribute::ContentLanguage, "en-US"),
                (Attribute::ContentType, "text/plain"),
                (Attribute::Metadata("team".into()), "storage"),
            ]);
            let put_attr_res = store
                .put_opts(
                    &attr_path,
                    Bytes::from_static(b"attrs").into(),
                    PutOptions {
                        attributes: attrs.clone(),
                        ..Default::default()
                    },
                )
                .await;
            if backend.rejects_attributes {
                let err = put_attr_res.unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for unsupported attributes, got {err:?}"
                );
            } else {
                put_attr_res.unwrap_or_else(|e| panic!("[{name}] put with attributes failed: {e}"));
                if backend.persists_attributes {
                    let got = store.get(&attr_path).await.unwrap();
                    assert_eq!(got.attributes, attrs, "[{name}] GET attributes mismatch");
                    let head = store
                        .get_opts(&attr_path, GetOptions::new().with_head(true))
                        .await
                        .unwrap();
                    assert_eq!(head.attributes, attrs, "[{name}] HEAD attributes mismatch");
                }
            }
        }
    })
    .await;
}

#[tokio::test]
async fn test_delete_and_delete_stream_across_backends() {
    with_timeout(async {
        for backend in build_backends() {
            let store = &backend.store;
            let name = backend.name;

            // Deleting an existing object removes it; deleting a missing object is idempotent.
            let single = Path::from("delete/single.txt");
            store
                .put(&single, Bytes::from_static(b"bye").into())
                .await
                .unwrap();
            store.delete(&single).await.unwrap();
            assert!(matches!(
                store.head(&single).await,
                Err(object_store::Error::NotFound { .. })
            ));
            store.delete(&single).await.unwrap_or_else(|e| {
                panic!("[{name}] idempotent delete on missing path failed: {e}")
            });

            // delete_stream deletes all paths and yields each deleted location.
            let paths = vec![
                Path::from("delete/batch/a.txt"),
                Path::from("delete/batch/b.txt"),
                Path::from("delete/batch/c space#1.txt"),
            ];
            for path in &paths {
                store
                    .put(path, Bytes::from_static(b"data").into())
                    .await
                    .unwrap();
            }
            let deleted: Vec<Path> = store
                .delete_stream(stream::iter(paths.clone().into_iter().map(Ok)).boxed())
                .try_collect()
                .await
                .unwrap_or_else(|e| panic!("[{name}] delete_stream failed: {e}"));
            assert_eq!(deleted, paths, "[{name}] delete_stream yielded wrong paths");
            for path in &paths {
                assert!(matches!(
                    store.head(path).await,
                    Err(object_store::Error::NotFound { .. })
                ));
            }

            // Empty delete_stream succeeds with no items.
            let empty: Vec<Path> = store
                .delete_stream(stream::empty().boxed())
                .try_collect()
                .await
                .unwrap();
            assert!(empty.is_empty());

            // Input stream errors propagate directly through delete_stream.
            let upstream_err = object_store::Error::Generic {
                store: "test",
                source: "upstream failure".into(),
            };
            let results: Vec<object_store::Result<Path>> = store
                .delete_stream(stream::iter(vec![Err(upstream_err)]).boxed())
                .collect()
                .await;
            assert_eq!(results.len(), 1);
            assert!(
                matches!(results[0], Err(object_store::Error::Generic { .. })),
                "[{name}] expected propagated upstream error, got {:?}",
                results[0]
            );
        }
    })
    .await;
}

#[tokio::test]
async fn test_copy_and_rename_across_backends() {
    with_timeout(async {
        for backend in build_backends() {
            let store = &backend.store;
            let cap = store.info().capability();
            let name = backend.name;

            let src = Path::from("move/src.txt");
            let dst = Path::from("move/dst.txt");
            let missing = Path::from("move/missing.txt");

            // copy (Overwrite)
            store
                .put(&src, Bytes::from_static(b"source").into())
                .await
                .unwrap();
            store
                .put(&dst, Bytes::from_static(b"old-target").into())
                .await
                .unwrap();
            if cap.copy {
                store
                    .copy(&src, &dst)
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] copy overwrite failed: {e}"));
                assert_eq!(
                    store.get(&src).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"source")
                );
                assert_eq!(
                    store.get(&dst).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"source")
                );

                // Same-path overwrite succeeds for existing file and returns NotFound for missing.
                store.copy(&src, &src).await.unwrap();
                assert!(matches!(
                    store.copy(&missing, &dst).await,
                    Err(object_store::Error::NotFound { .. })
                ));
            } else {
                let err = store.copy(&src, &dst).await.unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for copy, got {err:?}"
                );
            }

            // copy_if_not_exists (Create)
            let fresh = Path::from("move/copy_fresh.txt");
            if cap.copy_with_if_not_exists {
                store
                    .copy_if_not_exists(&src, &fresh)
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] copy_if_not_exists failed: {e}"));
                assert_eq!(
                    store.get(&fresh).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"source")
                );

                let exists_err = store.copy_if_not_exists(&src, &fresh).await.unwrap_err();
                assert!(
                    matches!(exists_err, object_store::Error::AlreadyExists { ref path, .. } if path == fresh.as_ref()),
                    "[{name}] expected AlreadyExists for copy_if_not_exists, got {exists_err:?}"
                );

                let missing_err = store
                    .copy_if_not_exists(&missing, &Path::from("move/never.txt"))
                    .await
                    .unwrap_err();
                assert!(
                    matches!(missing_err, object_store::Error::NotFound { .. }),
                    "[{name}] expected NotFound for copy_if_not_exists missing source, got {missing_err:?}"
                );
            } else {
                let err = store.copy_if_not_exists(&src, &fresh).await.unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for copy_if_not_exists, got {err:?}"
                );
            }

            // rename (Overwrite): uses native rename when available, otherwise copy + delete.
            let ren_src = Path::from("move/ren_src.txt");
            let ren_dst = Path::from("move/ren_dst.txt");
            store
                .put(&ren_src, Bytes::from_static(b"moved").into())
                .await
                .unwrap();
            store
                .put(&ren_dst, Bytes::from_static(b"existing").into())
                .await
                .unwrap();
            if cap.rename || cap.copy {
                store
                    .rename(&ren_src, &ren_dst)
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] rename overwrite failed: {e}"));
                assert_eq!(
                    store.get(&ren_dst).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"moved")
                );
                assert!(matches!(
                    store.head(&ren_src).await,
                    Err(object_store::Error::NotFound { .. })
                ));
                assert!(matches!(
                    store.rename(&missing, &ren_dst).await,
                    Err(object_store::Error::NotFound { .. })
                ));
            } else {
                let err = store.rename(&ren_src, &ren_dst).await.unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for rename, got {err:?}"
                );
                assert_eq!(
                    store.get(&ren_src).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"moved"),
                    "[{name}] failed rename must preserve source"
                );
            }

            // rename_if_not_exists (Create): uses native rename_with_if_not_exists or copy_with_if_not_exists + delete.
            let rne_src = Path::from("move/rne_src.txt");
            let rne_taken = Path::from("move/rne_taken.txt");
            let rne_free = Path::from("move/rne_free.txt");
            store
                .put(&rne_src, Bytes::from_static(b"rne-source").into())
                .await
                .unwrap();
            store
                .put(&rne_taken, Bytes::from_static(b"rne-taken").into())
                .await
                .unwrap();
            if cap.rename_with_if_not_exists || cap.copy_with_if_not_exists {
                let exists_err = store
                    .rename_if_not_exists(&rne_src, &rne_taken)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(exists_err, object_store::Error::AlreadyExists { ref path, .. } if path == rne_taken.as_ref()),
                    "[{name}] expected AlreadyExists for rename_if_not_exists, got {exists_err:?}"
                );
                assert_eq!(
                    store.get(&rne_src).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"rne-source")
                );
                assert_eq!(
                    store.get(&rne_taken).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"rne-taken")
                );

                store
                    .rename_if_not_exists(&rne_src, &rne_free)
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] rename_if_not_exists failed: {e}"));
                assert_eq!(
                    store.get(&rne_free).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"rne-source")
                );
                assert!(matches!(
                    store.head(&rne_src).await,
                    Err(object_store::Error::NotFound { .. })
                ));
            } else {
                let err = store
                    .rename_if_not_exists(&rne_src, &rne_free)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(err, object_store::Error::NotSupported { .. }),
                    "[{name}] expected NotSupported for rename_if_not_exists, got {err:?}"
                );
                assert_eq!(
                    store.get(&rne_src).await.unwrap().bytes().await.unwrap(),
                    Bytes::from_static(b"rne-source")
                );
            }
        }
    })
    .await;
}

#[tokio::test]
async fn test_list_operations_across_backends() {
    with_timeout(async {
        for backend in build_backends() {
            let store = &backend.store;
            let name = backend.name;

            let paths = vec![
                Path::from("root.txt"),
                Path::from("a/file1.txt"),
                Path::from("a/b/file2.txt"),
                Path::from("ab/file3.txt"),
                Path::from("order/a#b"),
                Path::from("order/a$b"),
                Path::from("order/a%23c"),
            ];
            for path in &paths {
                store
                    .put(path, Bytes::from_static(b"payload").into())
                    .await
                    .unwrap_or_else(|e| panic!("[{name}] put {path} failed: {e}"));
            }

            // Recursive list from root (None and explicit "/") returns all objects and no directories.
            let mut listed_none: Vec<Path> = store
                .list(None)
                .map_ok(|m| {
                    assert_eq!(m.size, 7, "[{name}] list metadata size mismatch");
                    m.location
                })
                .try_collect()
                .await
                .unwrap_or_else(|e| panic!("[{name}] list(None) failed: {e}"));
            listed_none.sort();
            let mut expected = paths.clone();
            expected.sort();
            assert_eq!(listed_none, expected, "[{name}] list(None) mismatch");

            let mut listed_root: Vec<Path> = store
                .list(Some(&Path::from("/")))
                .map_ok(|m| m.location)
                .try_collect()
                .await
                .unwrap_or_else(|e| panic!("[{name}] list(Some('/')) failed: {e}"));
            listed_root.sort();
            assert_eq!(listed_root, expected, "[{name}] list(Some('/')) mismatch");

            // Prefix list isolates `a/` from sibling `ab/`.
            let mut listed_a: Vec<Path> = store
                .list(Some(&Path::from("a")))
                .map_ok(|m| m.location)
                .try_collect()
                .await
                .unwrap();
            listed_a.sort();
            assert_eq!(
                listed_a,
                vec![Path::from("a/b/file2.txt"), Path::from("a/file1.txt")],
                "[{name}] list(Some('a')) mismatch"
            );

            // Nonexistent prefix yields empty stream.
            let listed_missing: Vec<Path> = store
                .list(Some(&Path::from("nonexistent")))
                .map_ok(|m| m.location)
                .try_collect()
                .await
                .unwrap();
            assert!(listed_missing.is_empty(), "[{name}] expected empty list");

            // list_with_offset compares encoded object_store::path::Path values:
            // "order/a$b" < "order/a%23b" (from "order/a#b") < "order/a%2523c" (from "order/a%23c").
            let offset = Path::from("order/a$b");
            let mut after_offset: Vec<Path> = store
                .list_with_offset(Some(&Path::from("order")), &offset)
                .map_ok(|m| m.location)
                .try_collect()
                .await
                .unwrap_or_else(|e| panic!("[{name}] list_with_offset failed: {e}"));
            after_offset.sort();
            assert_eq!(
                after_offset,
                vec![Path::from("order/a#b"), Path::from("order/a%23c")],
                "[{name}] list_with_offset must compare encoded paths and exclude offset"
            );

            // list_with_delimiter separates direct objects from immediate subdirectories.
            let root_delim = store
                .list_with_delimiter(None)
                .await
                .unwrap_or_else(|e| panic!("[{name}] list_with_delimiter(None) failed: {e}"));
            assert_eq!(
                root_delim
                    .objects
                    .iter()
                    .map(|m| m.location.clone())
                    .collect::<Vec<_>>(),
                vec![Path::from("root.txt")],
                "[{name}] root list_with_delimiter objects mismatch"
            );
            let mut root_prefixes = root_delim.common_prefixes;
            root_prefixes.sort();
            assert_eq!(
                root_prefixes,
                vec![Path::from("a"), Path::from("ab"), Path::from("order")],
                "[{name}] root list_with_delimiter prefixes mismatch"
            );

            let a_delim = store
                .list_with_delimiter(Some(&Path::from("a")))
                .await
                .unwrap();
            assert_eq!(
                a_delim
                    .objects
                    .iter()
                    .map(|m| m.location.clone())
                    .collect::<Vec<_>>(),
                vec![Path::from("a/file1.txt")],
                "[{name}] prefix 'a' objects mismatch"
            );
            assert_eq!(
                a_delim.common_prefixes,
                vec![Path::from("a/b")],
                "[{name}] prefix 'a' common_prefixes mismatch"
            );

            let missing_delim = store
                .list_with_delimiter(Some(&Path::from("nonexistent")))
                .await
                .unwrap();
            assert!(missing_delim.objects.is_empty());
            assert!(missing_delim.common_prefixes.is_empty());
        }
    })
    .await;
}

#[tokio::test]
async fn test_capability_override_changes_store_behavior() {
    with_timeout(async {
        let dir = TempDir::new().unwrap();
        let op = Operator::new(opendal::services::Fs::default().root(dir.path().to_str().unwrap()))
            .unwrap()
            .layer(
                CapabilityOverrideLayer::from_overrides(
                    "write_with_if_not_exists=false,rename=false,rename_with_if_not_exists=false",
                )
                .unwrap(),
            );
        let store = OpendalStore::new(op);

        // Disabled write_with_if_not_exists rejects PutMode::Create.
        let err = store
            .put_opts(
                &Path::from("created.txt"),
                Bytes::from_static(b"data").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, object_store::Error::NotSupported { .. }));

        // Disabled native rename falls back to copy + delete on Overwrite.
        let from = Path::from("from.txt");
        let to = Path::from("to.txt");
        store
            .put(&from, Bytes::from_static(b"fallback-move").into())
            .await
            .unwrap();
        store.rename(&from, &to).await.unwrap();
        assert_eq!(
            store.get(&to).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"fallback-move")
        );
        assert!(matches!(
            store.head(&from).await,
            Err(object_store::Error::NotFound { .. })
        ));

        // Disabled rename_with_if_not_exists + no copy_with_if_not_exists rejects Create rename.
        let err = store
            .rename_if_not_exists(&to, &Path::from("other.txt"))
            .await
            .unwrap_err();
        assert!(matches!(err, object_store::Error::NotSupported { .. }));
    })
    .await;
}

/// Minimal test layer over `Memory` providing an S3-like capability profile:
/// ETags, object versions, conditional writes (`if_match`, `if_version_match`),
/// versioned reads/stats, and conditional `copy` without native `rename`.
mod versioned_copy_layer {
    use super::*;

    #[derive(Clone, Debug)]
    pub struct VersionedCopyLayer;

    impl Layer for VersionedCopyLayer {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(VersionedCopyService {
                inner,
                state: Arc::new(Mutex::new(VersionedState::default())),
            })
        }
    }

    #[derive(Clone)]
    struct StoredVersion {
        etag: String,
        version: String,
        data: Bytes,
        last_modified: Timestamp,
    }

    #[derive(Default)]
    struct VersionedState {
        seq: u64,
        /// Per-path version history; the last entry is the current version.
        objects: HashMap<String, Vec<StoredVersion>>,
    }

    impl VersionedState {
        fn next_ids(&mut self) -> (String, String) {
            self.seq += 1;
            (format!("etag-{}", self.seq), format!("ver-{}", self.seq))
        }

        fn current(&self, path: &str) -> Option<&StoredVersion> {
            self.objects.get(path).and_then(|v| v.last())
        }

        fn by_version(&self, path: &str, version: &str) -> Option<&StoredVersion> {
            self.objects
                .get(path)
                .and_then(|v| v.iter().find(|entry| entry.version == version))
        }

        fn record(&mut self, path: &str, data: Bytes) -> StoredVersion {
            let (etag, version) = self.next_ids();
            let entry = StoredVersion {
                etag,
                version,
                data,
                last_modified: Timestamp::now(),
            };
            self.objects
                .entry(path.to_string())
                .or_default()
                .push(entry.clone());
            entry
        }
    }

    struct VersionedCopyService {
        inner: Servicer,
        state: Arc<Mutex<VersionedState>>,
    }

    impl Debug for VersionedCopyService {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            f.debug_struct("VersionedCopyService").finish()
        }
    }

    impl Service for VersionedCopyService {
        type Reader = VersionedReader;
        type Writer = VersionedWriter;
        type Lister = oio::Lister;
        type Deleter = VersionedDeleter;
        type Copier = VersionedCopier;
        type Composer = oio::Composer;

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> Capability {
            let mut cap = self.inner.capability();
            cap.read_with_version = true;
            cap.stat_with_version = true;
            cap.write_with_if_match = true;
            cap.write_with_if_version_match = true;
            cap.copy = true;
            cap.copy_with_if_not_exists = true;
            cap.rename = false;
            cap.rename_with_if_not_exists = false;
            cap
        }

        async fn create_dir(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> opendal::Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        async fn stat(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: OpStat,
        ) -> opendal::Result<RpStat> {
            if let Some(version) = args.version() {
                let state = self.state.lock().unwrap();
                let entry = state.by_version(path, version).ok_or_else(|| {
                    Error::new(ErrorKind::NotFound, "requested version not found")
                })?;
                let mut builder = MetadataBuilder::file(entry.data.len() as u64);
                builder
                    .etag(&entry.etag)
                    .version(&entry.version)
                    .last_modified(entry.last_modified);
                return Ok(RpStat::new(builder.build()));
            }

            let mut meta = self.inner.stat(ctx, path, args).await?.into_metadata();
            if meta.is_file()
                && let Some(entry) = self.state.lock().unwrap().current(path).cloned()
            {
                let mut builder = meta.into_builder();
                builder.etag(&entry.etag).version(&entry.version);
                meta = builder.build();
            }
            Ok(RpStat::new(meta))
        }

        fn read(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: OpRead,
        ) -> opendal::Result<Self::Reader> {
            if let Some(version) = args.version() {
                let entry = self
                    .state
                    .lock()
                    .unwrap()
                    .by_version(path, version)
                    .cloned()
                    .ok_or_else(|| {
                        Error::new(ErrorKind::NotFound, "requested version not found")
                    })?;
                return Ok(VersionedReader::Fixed(entry));
            }

            let inner = self.inner.read(ctx, path, args)?;
            Ok(VersionedReader::Inner {
                inner,
                path: path.to_string(),
                state: self.state.clone(),
            })
        }

        fn write(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<Self::Writer> {
            let expected_etag = args.if_match().map(str::to_string);
            let expected_version = args.if_version_match().map(str::to_string);
            let if_not_exists = args.if_not_exists();

            let inner = self.inner.write(ctx, path, args)?;
            Ok(VersionedWriter {
                inner,
                path: path.to_string(),
                state: self.state.clone(),
                expected_etag,
                expected_version,
                if_not_exists,
                buf: Vec::new(),
            })
        }

        fn delete(&self, ctx: &opendal::OperationContext) -> opendal::Result<Self::Deleter> {
            Ok(VersionedDeleter {
                inner: self.inner.delete(ctx)?,
                state: self.state.clone(),
            })
        }

        fn list(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: OpList,
        ) -> opendal::Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }

        fn copy(
            &self,
            ctx: &opendal::OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> opendal::Result<Self::Copier> {
            Ok(VersionedCopier {
                inner: self.inner.clone(),
                ctx: ctx.clone(),
                state: self.state.clone(),
                from: from.to_string(),
                to: to.to_string(),
                if_not_exists: args.if_not_exists(),
                done: false,
            })
        }

        async fn rename(
            &self,
            _: &opendal::OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> opendal::Result<RpRename> {
            Err(Error::new(ErrorKind::Unsupported, "rename not supported"))
        }

        async fn presign(
            &self,
            ctx: &opendal::OperationContext,
            path: &str,
            args: OpPresign,
        ) -> opendal::Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    enum VersionedReader {
        Fixed(StoredVersion),
        Inner {
            inner: oio::Reader,
            path: String,
            state: Arc<Mutex<VersionedState>>,
        },
    }

    struct FixedStream(Option<Buffer>);

    impl oio::ReadStream for FixedStream {
        async fn read(&mut self) -> opendal::Result<Buffer> {
            Ok(self.0.take().unwrap_or_default())
        }
    }

    impl oio::Read for VersionedReader {
        async fn open(
            &self,
            range: BytesRange,
        ) -> opendal::Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            match self {
                VersionedReader::Fixed(entry) => {
                    let size = entry.data.len() as u64;
                    let (start, end) = match range {
                        BytesRange::Range { offset, size: len } => {
                            let start = (offset as usize).min(entry.data.len());
                            let end = match len {
                                Some(len) => {
                                    start.saturating_add(len as usize).min(entry.data.len())
                                }
                                None => entry.data.len(),
                            };
                            (start, end)
                        }
                        BytesRange::Suffix { size: suffix } => {
                            let start = entry.data.len().saturating_sub(suffix as usize);
                            (start, entry.data.len())
                        }
                    };
                    let slice = entry.data.slice(start..end);
                    let mut meta = MetadataBuilder::file(size);
                    meta.etag(&entry.etag)
                        .version(&entry.version)
                        .last_modified(entry.last_modified);
                    Ok((
                        RpRead::new(meta.build()),
                        Box::new(FixedStream(Some(Buffer::from(slice)))),
                    ))
                }
                VersionedReader::Inner { inner, path, state } => {
                    let (rp, stream) = inner.open(range).await?;
                    if let Some(entry) = state.lock().unwrap().current(path).cloned() {
                        let meta = match rp.metadata() {
                            Some(existing) => {
                                let mut b = existing.clone().into_builder();
                                b.etag(&entry.etag).version(&entry.version);
                                b.build()
                            }
                            None => {
                                let mut b = MetadataBuilder::file(entry.data.len() as u64);
                                b.etag(&entry.etag)
                                    .version(&entry.version)
                                    .last_modified(entry.last_modified);
                                b.build()
                            }
                        };
                        return Ok((RpRead::new(meta), stream));
                    }
                    Ok((rp, stream))
                }
            }
        }

        async fn read(&self, range: BytesRange) -> opendal::Result<(RpRead, Buffer)> {
            let (rp, mut stream) = self.open(range).await?;
            let buf = stream.read_all_dyn().await?;
            Ok((rp, buf))
        }
    }

    struct VersionedWriter {
        inner: oio::Writer,
        path: String,
        state: Arc<Mutex<VersionedState>>,
        expected_etag: Option<String>,
        expected_version: Option<String>,
        if_not_exists: bool,
        buf: Vec<u8>,
    }

    impl oio::Write for VersionedWriter {
        async fn write(&mut self, bs: Buffer) -> opendal::Result<()> {
            self.buf.extend_from_slice(&bs.to_bytes());
            self.inner.write(bs).await
        }

        async fn close(&mut self) -> opendal::Result<opendal::Metadata> {
            {
                let state = self.state.lock().unwrap();
                let current = state.current(&self.path);
                if self.if_not_exists && current.is_some() {
                    return Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "object already exists",
                    ));
                }
                if let Some(expected) = &self.expected_etag
                    && current.map(|c| c.etag.as_str()) != Some(expected.as_str())
                {
                    return Err(Error::new(ErrorKind::ConditionNotMatch, "etag mismatch"));
                }
                if let Some(expected) = &self.expected_version
                    && current.map(|c| c.version.as_str()) != Some(expected.as_str())
                {
                    return Err(Error::new(ErrorKind::ConditionNotMatch, "version mismatch"));
                }
            }

            let meta = self.inner.close().await?;
            let entry = self
                .state
                .lock()
                .unwrap()
                .record(&self.path, Bytes::from(std::mem::take(&mut self.buf)));
            let mut builder = meta.into_builder();
            builder.etag(&entry.etag).version(&entry.version);
            Ok(builder.build())
        }

        async fn abort(&mut self) -> opendal::Result<()> {
            self.buf.clear();
            self.inner.abort().await
        }
    }

    struct VersionedDeleter {
        inner: oio::Deleter,
        state: Arc<Mutex<VersionedState>>,
    }

    impl oio::Delete for VersionedDeleter {
        async fn delete(&mut self, path: &str, args: OpDelete) -> opendal::Result<()> {
            self.state.lock().unwrap().objects.remove(path);
            self.inner.delete(path, args).await
        }

        async fn close(&mut self) -> opendal::Result<()> {
            self.inner.close().await
        }
    }

    struct VersionedCopier {
        inner: Servicer,
        ctx: opendal::OperationContext,
        state: Arc<Mutex<VersionedState>>,
        from: String,
        to: String,
        if_not_exists: bool,
        done: bool,
    }

    impl oio::Copy for VersionedCopier {
        async fn next(&mut self) -> opendal::Result<Option<usize>> {
            if self.done {
                return Ok(None);
            }
            let src_data = {
                let state = self.state.lock().unwrap();
                let src = state
                    .current(&self.from)
                    .ok_or_else(|| Error::new(ErrorKind::NotFound, "copy source not found"))?;
                if self.if_not_exists && state.current(&self.to).is_some() {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        "copy target already exists",
                    ));
                }
                src.data.clone()
            };

            let mut writer = self.inner.write(&self.ctx, &self.to, OpWrite::new())?;
            writer.write(Buffer::from(src_data.clone())).await?;
            writer.close().await?;
            self.state
                .lock()
                .unwrap()
                .record(&self.to, src_data.clone());
            self.done = true;
            Ok(Some(src_data.len()))
        }

        async fn close(&mut self) -> opendal::Result<opendal::Metadata> {
            let entry = self
                .state
                .lock()
                .unwrap()
                .current(&self.to)
                .cloned()
                .ok_or_else(|| Error::new(ErrorKind::Unexpected, "copy did not complete"))?;
            let mut builder = MetadataBuilder::file(entry.data.len() as u64);
            builder
                .etag(&entry.etag)
                .version(&entry.version)
                .last_modified(entry.last_modified);
            Ok(builder.build())
        }

        async fn abort(&mut self) -> opendal::Result<()> {
            Ok(())
        }
    }
}
