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

use anyhow::Result;
use bytes::Bytes;
use dav_server_opendalfs::OpendalFs;
use dav_server_opendalfs::dav_server::DavHandler;
use dav_server_opendalfs::dav_server::body::Body;
use dav_server_opendalfs::dav_server::davpath::DavPath;
use dav_server_opendalfs::dav_server::fs::{DavFile, FsError, OpenOptions};
use dav_server_opendalfs::dav_server::fs::{DavFileSystem, ReadDirMeta};
use futures::{StreamExt, TryStreamExt};
use http::{HeaderValue, Method, Request, Response, StatusCode, header::ETAG};
use opendal::services::{Fs, Http};
use opendal::{HttpBody, HttpTransport, HttpTransporter, Operator};
use opendal_core::raw::{
    OpCompose, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpRestore, OpStat,
    OpWrite, RpCreateDir, RpPresign, RpRename, RpRestore, RpStat, Service, ServiceDyn, ServiceInfo,
    Servicer, oio,
};
use opendal_core::{
    Buffer, Capability, Error, ErrorKind, Metadata, MetadataBuilder, OperationContext,
};
use std::fs;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn test() -> Result<()> {
    let builder = Fs::default().root("/tmp");

    let op = Operator::new(builder)?;

    let webdavfs = OpendalFs::new(op);

    let metadata = webdavfs
        .metadata(&DavPath::new("/").unwrap())
        .await
        .unwrap();
    println!("{}", metadata.is_dir());

    Ok(())
}

#[tokio::test]
async fn test_create_dir_nested() {
    let builder = opendal::services::Memory::default();
    let op = Operator::new(builder).unwrap();
    let webdavfs = OpendalFs::new(op.clone());

    webdavfs
        .create_dir(&DavPath::new("/a/").unwrap())
        .await
        .unwrap();
    webdavfs
        .create_dir(&DavPath::new("/a/b/").unwrap())
        .await
        .unwrap();

    assert!(op.exists("/a/").await.unwrap());
    assert!(op.exists("/a/b/").await.unwrap());
}

#[tokio::test]
async fn test_create_dir_root_is_exists() -> Result<()> {
    let memory = Operator::new(opendal::services::Memory::default())?;
    assert!(matches!(
        OpendalFs::new(memory).create_dir(&DavPath::new("/")?).await,
        Err(FsError::Exists)
    ));

    let root = tempfile::tempdir()?;
    let fs = Operator::new(Fs::default().root(root.path().to_str().unwrap()))?;
    assert!(matches!(
        OpendalFs::new(fs).create_dir(&DavPath::new("/")?).await,
        Err(FsError::Exists)
    ));
    Ok(())
}

#[tokio::test]
async fn test_create_dir_twice_is_exists() -> Result<()> {
    let op = Operator::new(opendal::services::Memory::default())?;
    let webdavfs = OpendalFs::new(op);

    webdavfs.create_dir(&DavPath::new("/dir")?).await?;
    assert!(matches!(
        webdavfs.create_dir(&DavPath::new("/dir")?).await,
        Err(FsError::Exists)
    ));
    Ok(())
}

#[tokio::test]
async fn test_mkcol_over_existing_file() -> Result<()> {
    let op = Operator::new(opendal::services::Memory::default())?;
    op.write("foo", "content").await?;

    let dav = DavHandler::builder()
        .filesystem(OpendalFs::new(op.clone()))
        .build_handler();
    for uri in ["/foo", "/foo/"] {
        let response = dav
            .handle(
                Request::builder()
                    .method("MKCOL")
                    .uri(uri)
                    .body(Body::empty())?,
            )
            .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{uri}");
    }

    for path in ["/foo", "/foo/"] {
        assert!(
            matches!(
                OpendalFs::new(op.clone())
                    .create_dir(&DavPath::new(path)?)
                    .await,
                Err(FsError::Exists)
            ),
            "{path}"
        );
    }

    assert_eq!(op.read("foo").await?.to_vec().as_slice(), b"content");
    assert!(!op.exists("foo/").await?);
    Ok(())
}

#[tokio::test]
async fn test_create_dir_under_file_is_rejected() -> Result<()> {
    let op = Operator::new(opendal::services::Memory::default())?;
    op.write("foo", "content").await?;
    let webdavfs = OpendalFs::new(op.clone());

    let err = webdavfs
        .create_dir(&DavPath::new("/foo/child")?)
        .await
        .unwrap_err();
    assert!(
        matches!(err, FsError::Forbidden | FsError::NotFound),
        "{err:?}"
    );

    assert_eq!(op.read("foo").await?.to_vec().as_slice(), b"content");
    assert!(!op.exists("foo/child/").await?);
    Ok(())
}

#[tokio::test]
async fn test_create_dir_over_existing_file_on_fs() -> Result<()> {
    let root = tempfile::tempdir()?;
    let op = Operator::new(Fs::default().root(root.path().to_str().unwrap()))?;
    op.write("foo", "content").await?;
    let webdavfs = OpendalFs::new(op.clone());

    assert!(matches!(
        webdavfs.create_dir(&DavPath::new("/foo")?).await,
        Err(FsError::Exists)
    ));

    assert_eq!(op.read("foo").await?.to_vec().as_slice(), b"content");
    assert!(root.path().join("foo").is_file());
    Ok(())
}

#[tokio::test]
async fn test_symlink_metadata() -> Result<()> {
    let op = Operator::new(opendal::services::Memory::default())?;
    op.create_dir("dir/").await?;
    op.write("file", "test").await?;
    let webdavfs = OpendalFs::new(op);

    for (path, is_dir, len) in [("/", true, 0), ("/dir/", true, 0), ("/file", false, 4)] {
        let path = DavPath::new(path)?;
        let metadata = webdavfs.symlink_metadata(&path).await?;
        assert_eq!(metadata.is_dir(), is_dir);
        assert_eq!(metadata.is_file(), !is_dir);
        assert_eq!(metadata.len(), len);
        assert!(!metadata.is_symlink());
    }

    assert!(matches!(
        webdavfs.symlink_metadata(&DavPath::new("/missing")?).await,
        Err(FsError::NotFound)
    ));
    Ok(())
}

#[tokio::test]
async fn test_file_etag() -> Result<()> {
    const CONTENT: &[u8] = b"hello";
    struct EtagTransport(&'static str);

    impl HttpTransport for EtagTransport {
        async fn fetch(&self, req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            let body = match *req.method() {
                Method::HEAD => HttpBody::new(futures::stream::empty(), Some(0)),
                Method::GET => HttpBody::new(
                    futures::stream::iter([Ok(Buffer::from(CONTENT))]),
                    Some(CONTENT.len() as u64),
                ),
                ref method => panic!("unexpected storage request: {method}"),
            };
            Ok(Response::builder()
                .header(ETAG, self.0)
                .header("content-length", CONTENT.len())
                .body(body)
                .unwrap())
        }
    }

    for (etag, expected_tag) in [
        (r#""abc""#, Some("abc")),
        ("abc", Some("abc")),
        ("\"\"", Some("")),
        ("W/abc", Some("W/abc")),
        (r#""W/abc""#, Some("W/abc")),
        (r#"W/"abc""#, None),
        ("\"abc", None),
        ("abc\"", None),
        (r#""a"b""#, None),
    ] {
        let expected_header = expected_tag.map(|tag| format!("\"{tag}\""));
        let ctx =
            OperationContext::new().with_http_transport(HttpTransporter::new(EtagTransport(etag)));
        let op = Operator::new(Http::default().endpoint("http://example.test"))?.with_context(ctx);
        let dav = DavHandler::builder()
            .filesystem(OpendalFs::new(op))
            .build_handler();
        for (method, contents) in [(Method::GET, CONTENT), (Method::HEAD, &[][..])] {
            let req = Request::builder()
                .method(method)
                .uri("/file")
                .body(Body::empty())?;
            let resp = dav.handle(req).await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(
                resp.headers()
                    .get(ETAG)
                    .map(HeaderValue::to_str)
                    .transpose()?,
                expected_header.as_deref()
            );
            let body = resp
                .into_body()
                .map_ok(|chunk| chunk.to_vec())
                .try_concat()
                .await?;
            assert_eq!(body, contents);
        }

        let req = Request::builder()
            .method("PROPFIND")
            .uri("/file")
            .header("Depth", "0")
            .body(Body::empty())?;
        let resp = dav.handle(req).await;
        assert_eq!(resp.status(), StatusCode::MULTI_STATUS);
        let body = resp
            .into_body()
            .map_ok(|chunk| chunk.to_vec())
            .try_concat()
            .await?;
        let xml = String::from_utf8(body)?;
        match expected_header {
            Some(value) => assert!(
                xml.contains(&format!("<D:getetag>{value}</D:getetag>")),
                "{xml}"
            ),
            None => assert!(!xml.contains("getetag"), "{xml}"),
        }
    }
    Ok(())
}

fn setup_temp(path: &str) -> Box<OpendalFs> {
    let _ = fs::remove_dir_all(path);
    let builder = Fs::default().root(path);
    let op = Operator::new(builder).unwrap();
    OpendalFs::new(op)
}

const TEST_PATH_ENCODED_1: &str = "test_%CE%B1%CE%BB%CF%86%CE%AC";
const TEST_PATH_DECODED_1: &str = "test_αλφά";
const TEST_PATH_ENCODED_2: &str = "test_%CE%B2%CE%B7%CF%84%CE%BF";
const TEST_PATH_DECODED_2: &str = "test_βητο";

#[tokio::test]
async fn test_create_dir_metadata() {
    const TMP_PATH: &str = "/tmp/test_create_dir_metadata";
    let webdavfs = setup_temp(TMP_PATH);
    webdavfs
        .create_dir(&DavPath::new(&format!("/{TEST_PATH_ENCODED_1}/")).unwrap())
        .await
        .unwrap();
    assert!(fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_1)).unwrap());
    assert!(!fs::exists(Path::new(TMP_PATH).join(TEST_PATH_ENCODED_1)).unwrap());

    webdavfs
        .metadata(&DavPath::new(&format!("/{TEST_PATH_ENCODED_1}/")).unwrap())
        .await
        .unwrap();
    fs::remove_dir_all(TMP_PATH).unwrap();
}

#[tokio::test]
async fn test_remove_dir() {
    const TMP_PATH: &str = "/tmp/test_remove_dir";
    let webdavfs = setup_temp(TMP_PATH);
    webdavfs
        .create_dir(&DavPath::new(&format!("/{TEST_PATH_ENCODED_1}/")).unwrap())
        .await
        .unwrap();
    webdavfs
        .remove_dir(&DavPath::new(&format!("/{TEST_PATH_ENCODED_1}/")).unwrap())
        .await
        .unwrap();
    assert!(!fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_1)).unwrap());
    assert!(!fs::exists(Path::new(TMP_PATH).join(TEST_PATH_ENCODED_1)).unwrap());
    fs::remove_dir_all("/tmp/test_remove_dir").unwrap();
}
#[tokio::test]
async fn test_file() {
    const TMP_PATH: &str = "/tmp/test_file";
    let webdavfs = setup_temp(TMP_PATH);

    let mut f1 = webdavfs
        .open(
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_1}")).unwrap(),
            OpenOptions {
                write: true,
                create_new: true,
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();
    f1.write_buf(Box::new(Bytes::from("test"))).await.unwrap();
    f1.flush().await.unwrap();
    drop(f1);
    assert!(fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_1)).unwrap());

    let mut f1 = webdavfs
        .open(
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_1}")).unwrap(),
            OpenOptions {
                read: true,
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(f1.read_bytes(4).await.unwrap(), Bytes::from("test"));
    drop(f1);

    webdavfs
        .rename(
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_1}")).unwrap(),
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_2}")).unwrap(),
        )
        .await
        .unwrap();
    assert!(!fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_1)).unwrap());
    assert!(fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_2)).unwrap());

    webdavfs
        .copy(
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_2}")).unwrap(),
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_1}")).unwrap(),
        )
        .await
        .unwrap();
    assert!(fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_1)).unwrap());
    assert!(fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_2)).unwrap());

    webdavfs
        .remove_file(&DavPath::new(&format!("/{TEST_PATH_ENCODED_1}")).unwrap())
        .await
        .unwrap();
    assert!(!fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_1)).unwrap());
    assert!(fs::exists(Path::new(TMP_PATH).join(TEST_PATH_DECODED_2)).unwrap());

    fs::remove_dir_all(TMP_PATH).unwrap();
}

#[tokio::test]
async fn test_read_dir() {
    const TMP_PATH: &str = "/tmp/test_read_dir";
    let webdavfs = setup_temp(TMP_PATH);
    webdavfs
        .create_dir(&DavPath::new(&format!("/{TEST_PATH_ENCODED_1}/")).unwrap())
        .await
        .unwrap();
    webdavfs
        .create_dir(
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_1}/{TEST_PATH_ENCODED_1}/")).unwrap(),
        )
        .await
        .unwrap();
    webdavfs
        .create_dir(
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_1}/{TEST_PATH_ENCODED_2}/")).unwrap(),
        )
        .await
        .unwrap();
    let entries = webdavfs
        .read_dir(
            &DavPath::new(&format!("/{TEST_PATH_ENCODED_1}/")).unwrap(),
            ReadDirMeta::None,
        )
        .await
        .unwrap();
    let entries = entries
        .collect::<Vec<_>>()
        .await
        .iter()
        .map(|entry| String::from_utf8(entry.as_ref().unwrap().name()).unwrap())
        .collect::<Vec<_>>();
    println!("{entries:?}");
    assert_eq!(entries.len(), 2);
    assert!(entries.contains(&format!("{TEST_PATH_DECODED_1}/")));
    assert!(entries.contains(&format!("{TEST_PATH_DECODED_2}/")));

    fs::remove_dir_all(TMP_PATH).unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Failure {
    Write,
    Close,
}

#[derive(Debug)]
struct AbortTrackingService {
    failure: Failure,
    aborted: Arc<AtomicBool>,
}

impl Service for AbortTrackingService {
    type Reader = ();
    type Writer = AbortTrackingWriter;
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("abort-tracking")
    }

    fn capability(&self) -> Capability {
        Capability {
            write: true,
            write_can_append: true,
            ..Default::default()
        }
    }

    fn write(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpWrite,
    ) -> opendal_core::Result<Self::Writer> {
        Ok(AbortTrackingWriter {
            failure: self.failure,
            aborted: self.aborted.clone(),
        })
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal_core::Result<RpCreateDir> {
        ().create_dir(ctx, path, args).await
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal_core::Result<RpStat> {
        ().stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> opendal_core::Result<()> {
        ().read(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> opendal_core::Result<()> {
        ().delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> opendal_core::Result<()> {
        ().list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> opendal_core::Result<()> {
        ().copy(ctx, from, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal_core::Result<RpRename> {
        ().rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal_core::Result<RpPresign> {
        ().presign(ctx, path, args).await
    }
}

struct AbortTrackingWriter {
    failure: Failure,
    aborted: Arc<AtomicBool>,
}

impl oio::Write for AbortTrackingWriter {
    async fn write(&mut self, _: Buffer) -> opendal_core::Result<()> {
        if matches!(self.failure, Failure::Write) {
            return Err(Error::new(ErrorKind::Unexpected, "injected write failure"));
        }
        Ok(())
    }

    async fn close(&mut self) -> opendal_core::Result<Metadata> {
        if matches!(self.failure, Failure::Close) {
            return Err(Error::new(ErrorKind::Unexpected, "injected close failure"));
        }
        Ok(MetadataBuilder::unknown().build())
    }

    async fn abort(&mut self) -> opendal_core::Result<()> {
        self.aborted.store(true, Ordering::Relaxed);
        Ok(())
    }
}

async fn open_file_with_failure(
    failure: Failure,
    append: bool,
) -> (Box<dyn DavFile>, Arc<AtomicBool>) {
    let aborted = Arc::new(AtomicBool::new(false));
    let service = AbortTrackingService {
        failure,
        aborted: aborted.clone(),
    };
    let op = Operator::from_parts(OperationContext::default(), Arc::new(service));
    let file = OpendalFs::new(op)
        .open(
            &DavPath::new("/failed-upload").unwrap(),
            OpenOptions {
                write: true,
                append,
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();
    (file, aborted)
}

#[tokio::test]
async fn test_failed_write_aborts_before_returning() {
    for append in [false, true] {
        for write_buf in [false, true] {
            let (mut file, aborted) = open_file_with_failure(Failure::Write, append).await;
            // Exercise the backend write even if the adapter buffers incoming bytes.
            let body = Bytes::from(vec![1; 1024 * 1024]);
            let result = if write_buf {
                file.write_buf(Box::new(body)).await
            } else {
                file.write_bytes(body).await
            };

            assert!(matches!(result, Err(FsError::GeneralFailure)));
            assert!(
                aborted.load(Ordering::Relaxed),
                "write failure must abort the writer"
            );
            assert!(matches!(file.flush().await, Err(FsError::GeneralFailure)));
            assert!(matches!(
                file.seek(SeekFrom::Current(0)).await,
                Err(FsError::GeneralFailure)
            ));
        }
    }
}

#[tokio::test]
async fn test_failed_close_aborts_before_returning() {
    for append in [false, true] {
        let (mut file, aborted) = open_file_with_failure(Failure::Close, append).await;
        file.write_bytes(Bytes::from_static(b"body")).await.unwrap();

        assert!(matches!(file.flush().await, Err(FsError::GeneralFailure)));
        assert!(
            aborted.load(Ordering::Relaxed),
            "close failure must abort the writer"
        );
        assert!(matches!(
            file.seek(SeekFrom::Current(0)).await,
            Err(FsError::GeneralFailure)
        ));
    }
}

/// Wrapper that advertises `rename` but rejects it with `IsADirectory`,
/// mirroring backends such as `smb` that cannot move directories. It can also
/// hide one path from `stat` for a single call to simulate a destination
/// collection appearing between the adapter's existence check and rename.
#[derive(Debug)]
struct DirRenameDenyService {
    inner: Servicer,
    hidden_path: String,
    hidden: AtomicBool,
}

impl Service for DirRenameDenyService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info_dyn()
    }

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability_dyn();
        capability.rename = true;
        capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal_core::Result<RpCreateDir> {
        self.inner.create_dir_dyn(ctx, path, args).await
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal_core::Result<RpStat> {
        if path == self.hidden_path && self.hidden.swap(false, Ordering::Relaxed) {
            return Err(Error::new(ErrorKind::NotFound, "hidden for test"));
        }
        self.inner.stat_dyn(ctx, path, args).await
    }

    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal_core::Result<Self::Reader> {
        self.inner.read_dyn(ctx, path, args)
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal_core::Result<Self::Writer> {
        self.inner.write_dyn(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> opendal_core::Result<Self::Deleter> {
        self.inner.delete_dyn(ctx)
    }

    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal_core::Result<Self::Lister> {
        self.inner.list_dyn(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> opendal_core::Result<Self::Copier> {
        self.inner.copy_dyn(ctx, from, to, args)
    }

    fn compose(
        &self,
        ctx: &OperationContext,
        to: &str,
        args: OpCompose,
    ) -> opendal_core::Result<Self::Composer> {
        self.inner.compose_dyn(ctx, to, args)
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> opendal_core::Result<RpRename> {
        Err(Error::new(
            ErrorKind::IsADirectory,
            "cannot rename a directory",
        ))
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> opendal_core::Result<RpRestore> {
        self.inner.restore_dyn(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal_core::Result<RpPresign> {
        self.inner.presign_dyn(ctx, path, args).await
    }
}

fn deny_dir_rename_op(hidden_path: Option<&str>) -> Operator {
    let inner = Operator::new(opendal::services::Memory::default())
        .unwrap()
        .service()
        .clone();
    Operator::from_parts(
        OperationContext::default(),
        Arc::new(DirRenameDenyService {
            inner,
            hidden_path: hidden_path.unwrap_or_default().to_string(),
            hidden: AtomicBool::new(hidden_path.is_some()),
        }),
    )
}

#[tokio::test]
async fn test_collection_move_falls_back_when_backend_denies_dir_rename() -> Result<()> {
    let webdavfs = OpendalFs::new(deny_dir_rename_op(None));

    // An empty collection falls back to create + delete.
    webdavfs
        .create_dir(&DavPath::new("/empty/").unwrap())
        .await
        .unwrap();
    webdavfs
        .rename(
            &DavPath::new("/empty/").unwrap(),
            &DavPath::new("/renamed/").unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        webdavfs.metadata(&DavPath::new("/renamed/")?).await,
        Ok(metadata) if metadata.is_dir()
    ));

    // A non-empty collection still reports NotImplemented.
    webdavfs
        .create_dir(&DavPath::new("/dir/").unwrap())
        .await
        .unwrap();
    webdavfs.op.write("dir/file", b"x".to_vec()).await?;
    assert!(matches!(
        webdavfs
            .rename(
                &DavPath::new("/dir/").unwrap(),
                &DavPath::new("/moved/").unwrap(),
            )
            .await,
        Err(FsError::NotImplemented)
    ));

    // Moving a missing collection reports NotFound.
    assert!(matches!(
        webdavfs
            .rename(
                &DavPath::new("/gone/").unwrap(),
                &DavPath::new("/nowhere/").unwrap(),
            )
            .await,
        Err(FsError::NotFound)
    ));
    Ok(())
}

#[tokio::test]
async fn test_collection_move_denies_root() -> Result<()> {
    for op in [
        deny_dir_rename_op(None),
        Operator::new(opendal::services::Memory::default()).unwrap(),
    ] {
        let webdavfs = OpendalFs::new(op);
        assert!(matches!(
            webdavfs
                .rename(&DavPath::new("/")?, &DavPath::new("/moved/")?)
                .await,
            Err(FsError::Forbidden)
        ));
        webdavfs
            .create_dir(&DavPath::new("/dir/").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            webdavfs
                .rename(&DavPath::new("/dir/")?, &DavPath::new("/")?)
                .await,
            Err(FsError::Forbidden)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn test_collection_move_into_existing_dir_is_exists() -> Result<()> {
    // `/moved/` exists but is hidden from the first stat, so the adapter's
    // existence check sees a free destination and only the rename-time
    // IsADirectory re-check observes it.
    let webdavfs = OpendalFs::new(deny_dir_rename_op(Some("moved/")));
    webdavfs
        .create_dir(&DavPath::new("/dir/").unwrap())
        .await
        .unwrap();
    webdavfs
        .create_dir(&DavPath::new("/moved/").unwrap())
        .await
        .unwrap();

    assert!(matches!(
        webdavfs
            .rename(
                &DavPath::new("/dir/").unwrap(),
                &DavPath::new("/moved/").unwrap(),
            )
            .await,
        Err(FsError::Exists)
    ));

    // Neither collection is touched by the refused move.
    for path in ["/dir/", "/moved/"] {
        assert!(matches!(
            webdavfs.metadata(&DavPath::new(path)?).await,
            Ok(metadata) if metadata.is_dir()
        ));
    }
    Ok(())
}

#[tokio::test]
async fn test_collection_move_without_backend_rename() -> Result<()> {
    // Memory has no native rename; the empty-collection move keeps working.
    let op = Operator::new(opendal::services::Memory::default()).unwrap();
    let webdavfs = OpendalFs::new(op);

    webdavfs
        .create_dir(&DavPath::new("/empty/").unwrap())
        .await
        .unwrap();
    webdavfs
        .rename(
            &DavPath::new("/empty/").unwrap(),
            &DavPath::new("/renamed/").unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        webdavfs.metadata(&DavPath::new("/renamed/")?).await,
        Ok(metadata) if metadata.is_dir()
    ));

    webdavfs
        .create_dir(&DavPath::new("/dir/").unwrap())
        .await
        .unwrap();
    webdavfs.op.write("dir/file", b"x".to_vec()).await?;
    assert!(matches!(
        webdavfs
            .rename(
                &DavPath::new("/dir/").unwrap(),
                &DavPath::new("/moved/").unwrap(),
            )
            .await,
        Err(FsError::NotImplemented)
    ));
    Ok(())
}

#[tokio::test]
async fn test_append_seek_and_cursor_accounting() -> Result<()> {
    let root = tempfile::tempdir()?;
    let builder = Fs::default().root(root.path().to_str().unwrap());
    let op = Operator::new(builder)?;
    op.write("append.txt", "abc").await?;

    let webdavfs = OpendalFs::new(op.clone());
    let path = DavPath::new("/append.txt")?;

    let mut file = webdavfs
        .open(
            &path,
            OpenOptions {
                write: true,
                append: true,
                ..OpenOptions::default()
            },
        )
        .await?;

    // Before writing, Start(0) (used by dav-server), Start(eof), Current(0), and End(0)
    // all succeed and report the current append offset.
    assert_eq!(file.seek(SeekFrom::Start(0)).await?, 3);
    assert_eq!(file.seek(SeekFrom::Current(0)).await?, 3);
    assert_eq!(file.seek(SeekFrom::End(0)).await?, 3);
    assert_eq!(file.seek(SeekFrom::Start(3)).await?, 3);

    // Random seeks away from the append position must fail.
    assert!(matches!(
        file.seek(SeekFrom::Start(1)).await,
        Err(FsError::NotImplemented)
    ));
    assert!(matches!(
        file.seek(SeekFrom::Current(-1)).await,
        Err(FsError::NotImplemented)
    ));
    assert!(matches!(
        file.seek(SeekFrom::End(-1)).await,
        Err(FsError::NotImplemented)
    ));

    file.write_bytes(Bytes::from_static(b"d")).await?;
    assert_eq!(file.seek(SeekFrom::Current(0)).await?, 4);
    assert_eq!(file.seek(SeekFrom::End(0)).await?, 4);
    assert_eq!(file.seek(SeekFrom::Start(4)).await?, 4);
    assert!(matches!(
        file.seek(SeekFrom::Start(0)).await,
        Err(FsError::NotImplemented)
    ));

    file.write_buf(Box::new(Bytes::from_static(b"ef"))).await?;
    assert_eq!(file.seek(SeekFrom::Current(0)).await?, 6);

    file.flush().await?;
    assert!(matches!(
        file.seek(SeekFrom::Current(0)).await,
        Err(FsError::GeneralFailure)
    ));
    assert!(matches!(file.flush().await, Err(FsError::GeneralFailure)));

    assert_eq!(op.read("append.txt").await?.to_bytes().as_ref(), b"abcdef");

    // Non-append writers reject all seeks (including Start(0)) without modifying the file.
    let mut writer = webdavfs
        .open(
            &path,
            OpenOptions {
                write: true,
                ..OpenOptions::default()
            },
        )
        .await?;
    assert!(matches!(
        writer.seek(SeekFrom::Start(0)).await,
        Err(FsError::NotImplemented)
    ));
    assert!(matches!(
        writer.seek(SeekFrom::Start(2)).await,
        Err(FsError::NotImplemented)
    ));
    drop(writer);

    assert_eq!(op.read("append.txt").await?.to_bytes().as_ref(), b"abcdef");
    Ok(())
}
