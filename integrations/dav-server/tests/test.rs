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
use http::{HeaderValue, Method, Request, Response, StatusCode, header, header::ETAG};
use http_body_util::BodyExt;
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
    assert!(entries.contains(&TEST_PATH_DECODED_1.to_string()));
    assert!(entries.contains(&TEST_PATH_DECODED_2.to_string()));

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
async fn test_append_accepts_initial_seek_to_start() -> Result<()> {
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

    file.seek(SeekFrom::Start(0)).await?;
    assert!(file.seek(SeekFrom::Start(1)).await.is_err());

    file.write_bytes(Bytes::from_static(b"d")).await?;
    assert!(file.seek(SeekFrom::Start(0)).await.is_err());
    file.write_buf(Box::new(Bytes::from_static(b"ef"))).await?;

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
    assert!(writer.seek(SeekFrom::Start(0)).await.is_err());
    assert!(writer.seek(SeekFrom::Start(2)).await.is_err());
    drop(writer);

    assert_eq!(op.read("append.txt").await?.to_bytes().as_ref(), b"abcdef");
    Ok(())
}

async fn dav_request(dav: &DavHandler, req: Request<Body>) -> Result<(StatusCode, Bytes)> {
    let resp = dav.handle(req).await;
    let status = resp.status();
    let body = BodyExt::collect(resp.into_body()).await?.to_bytes();
    Ok((status, body))
}

fn propfind_is_collection(xml: &[u8]) -> Result<bool> {
    let text = std::str::from_utf8(xml)?;
    let doc = roxmltree::Document::parse(text)?;
    Ok(doc
        .descendants()
        .any(|node| node.tag_name().name() == "collection"))
}

async fn assert_put_and_patch_on_collection_rejected(op: Operator, label: &str) -> Result<()> {
    const SABREDAV_PARTIAL_UPDATE: &str = "application/x-sabredav-partialupdate";

    let dav = DavHandler::builder()
        .filesystem(OpendalFs::new(op.clone()))
        .build_handler();

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("MKCOL")
            .uri("/dir/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{label}: MKCOL /dir/");

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::PUT)
            .uri("/dir/child.txt")
            .body(Body::from(Bytes::from_static(b"keep")))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{label}: PUT /dir/child.txt");

    // PUT on the collection URI (without or with trailing slash) must be rejected
    // rather than creating a shadowing file key.
    for uri in ["/dir", "/dir/"] {
        let (status, _) = dav_request(
            &dav,
            Request::builder()
                .method(Method::PUT)
                .uri(uri)
                .body(Body::from(Bytes::from_static(b"shadow")))?,
        )
        .await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: PUT {uri}");
    }

    // SabreDAV append PATCH targeting the collection must also be rejected.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::PATCH)
            .uri("/dir")
            .header(header::CONTENT_TYPE, SABREDAV_PARTIAL_UPDATE)
            .header("X-Update-Range", "append")
            .header(header::CONTENT_LENGTH, "6")
            .body(Body::from(Bytes::from_static(b"shadow")))?,
    )
    .await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{label}: PATCH /dir");

    // Opening a collection for writing must fail.
    let webdavfs = OpendalFs::new(op.clone());
    let dir_path = DavPath::new("/dir")?;
    assert!(
        matches!(
            webdavfs
                .open(
                    &dir_path,
                    OpenOptions {
                        write: true,
                        ..OpenOptions::default()
                    },
                )
                .await,
            Err(FsError::Forbidden)
        ),
        "{label}: open(write) /dir"
    );
    assert!(
        matches!(
            webdavfs
                .open(
                    &dir_path,
                    OpenOptions {
                        write: true,
                        append: true,
                        ..OpenOptions::default()
                    },
                )
                .await,
            Err(FsError::Forbidden)
        ),
        "{label}: open(append) /dir"
    );
    assert!(
        matches!(
            webdavfs
                .open(
                    &dir_path,
                    OpenOptions {
                        write: true,
                        create_new: true,
                        ..OpenOptions::default()
                    },
                )
                .await,
            Err(FsError::Exists)
        ),
        "{label}: open(create_new) /dir"
    );

    // No shadowing file key was created, PROPFIND still reports a collection,
    // and the existing child content is intact.
    assert!(
        !matches!(op.stat("dir").await, Ok(meta) if meta.is_file()),
        "{label}: dir must not exist as a file"
    );
    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method("PROPFIND")
            .uri("/dir")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{label}: PROPFIND /dir");
    assert!(
        propfind_is_collection(&body)?,
        "{label}: PROPFIND /dir must still report a collection"
    );

    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method(Method::GET)
            .uri("/dir/child.txt")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{label}: GET /dir/child.txt");
    assert_eq!(body.as_ref(), b"keep", "{label}: child content");
    Ok(())
}

#[tokio::test]
async fn test_put_and_patch_on_existing_collection_rejected() -> Result<()> {
    let memory = Operator::new(opendal::services::Memory::default())?;
    assert_put_and_patch_on_collection_rejected(memory, "memory").await?;

    let root = tempfile::tempdir()?;
    let fs_op = Operator::new(Fs::default().root(root.path().to_str().unwrap()))?;
    assert_put_and_patch_on_collection_rejected(fs_op, "fs").await?;
    Ok(())
}

#[tokio::test]
async fn test_put_and_open_on_implicit_collection_rejected() -> Result<()> {
    const SABREDAV_PARTIAL_UPDATE: &str = "application/x-sabredav-partialupdate";

    let op = Operator::new(opendal::services::Memory::default())?;
    let dav = DavHandler::builder()
        .filesystem(OpendalFs::new(op.clone()))
        .build_handler();

    // Create only a nested file without MKCOL /implicit/ to form an implicit prefix.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::PUT)
            .uri("/implicit/child.txt")
            .body(Body::from(Bytes::from_static(b"keep")))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::PUT)
            .uri("/implicit")
            .body(Body::from(Bytes::from_static(b"shadow")))?,
    )
    .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::PATCH)
            .uri("/implicit")
            .header(header::CONTENT_TYPE, SABREDAV_PARTIAL_UPDATE)
            .header("X-Update-Range", "append")
            .header(header::CONTENT_LENGTH, "6")
            .body(Body::from(Bytes::from_static(b"shadow")))?,
    )
    .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let webdavfs = OpendalFs::new(op.clone());

    assert!(matches!(
        op.stat("implicit").await,
        Err(err) if err.kind() == ErrorKind::NotFound
    ));
    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method("PROPFIND")
            .uri("/implicit")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::MULTI_STATUS);
    assert!(propfind_is_collection(&body)?);

    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method(Method::GET)
            .uri("/implicit/child.txt")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"keep");
    Ok(())
}

async fn assert_delete_depth_zero_and_missing(op: Operator, label: &str) -> Result<()> {
    let dav = DavHandler::builder()
        .filesystem(OpendalFs::new(op.clone()))
        .build_handler();

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("MKCOL")
            .uri("/dir/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{label}: MKCOL /dir/");

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::PUT)
            .uri("/dir/child.txt")
            .body(Body::from(Bytes::from_static(b"keep")))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{label}: PUT /dir/child.txt");

    // Depth: 0 DELETE on a non-empty collection must fail and keep children intact.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::DELETE)
            .uri("/dir/")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(
        status,
        StatusCode::METHOD_NOT_ALLOWED,
        "{label}: DELETE /dir/ Depth: 0 on non-empty collection"
    );

    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method(Method::GET)
            .uri("/dir/child.txt")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{label}: child survives");
    assert_eq!(body.as_ref(), b"keep");

    // Direct remove_dir must refuse a non-empty collection.
    let webdavfs = OpendalFs::new(op.clone());
    assert!(
        matches!(
            webdavfs.remove_dir(&DavPath::new("/dir/")?).await,
            Err(FsError::Exists)
        ),
        "{label}: remove_dir non-empty"
    );
    // After removing the child, Depth: 0 DELETE removes the now-empty collection.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::DELETE)
            .uri("/dir/child.txt")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT, "{label}: DELETE child");

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::DELETE)
            .uri("/dir/")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "{label}: DELETE empty /dir/ Depth: 0"
    );

    // Deleting a missing collection returns 404 for both Depth: 0 and default Depth.
    for depth in [Some("0"), None] {
        let mut req = Request::builder().method(Method::DELETE).uri("/missing/");
        if let Some(depth) = depth {
            req = req.header("Depth", depth);
        }
        let (status, _) = dav_request(&dav, req.body(Body::empty())?).await?;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{label}: DELETE /missing/ depth={depth:?}"
        );
    }

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::DELETE)
            .uri("/dir/")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "{label}: DELETE already-removed /dir/"
    );
    Ok(())
}

#[tokio::test]
async fn test_delete_collection_depth_zero_and_missing() -> Result<()> {
    let memory = Operator::new(opendal::services::Memory::default())?;
    assert_delete_depth_zero_and_missing(memory, "memory").await?;

    let root = tempfile::tempdir()?;
    let fs_op = Operator::new(Fs::default().root(root.path().to_str().unwrap()))?;
    assert_delete_depth_zero_and_missing(fs_op, "fs").await?;
    Ok(())
}

#[tokio::test]
async fn test_delete_implicit_collection_depth_zero_and_infinity() -> Result<()> {
    let op = Operator::new(opendal::services::Memory::default())?;
    let dav = DavHandler::builder()
        .filesystem(OpendalFs::new(op))
        .build_handler();

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::PUT)
            .uri("/implicit/child.txt")
            .body(Body::from(Bytes::from_static(b"keep")))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    // Depth: 0 must refuse an implicit non-empty collection.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::DELETE)
            .uri("/implicit/")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);

    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method(Method::GET)
            .uri("/implicit/child.txt")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"keep");

    // Default Depth: infinity still deletes an implicit collection cleanly.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::DELETE)
            .uri("/implicit/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::GET)
            .uri("/implicit/child.txt")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Recursive DELETE of an explicit collection containing an implicit subdir
    // must also succeed cleanly.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("MKCOL")
            .uri("/outer/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::PUT)
            .uri("/outer/sub/child.txt")
            .body(Body::from(Bytes::from_static(b"deep")))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::DELETE)
            .uri("/outer/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("PROPFIND")
            .uri("/outer/")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

struct CopyFromInnerCopier {
    op: Operator,
    from: String,
    to: String,
    copied: Option<usize>,
}

impl oio::Copy for CopyFromInnerCopier {
    async fn next(&mut self) -> opendal_core::Result<Option<usize>> {
        if self.copied.is_some() {
            return Ok(None);
        }
        let data = self.op.read(&self.from).await?;
        let len = data.len();
        self.op.write(&self.to, data).await?;
        self.copied = Some(len);
        Ok(Some(len))
    }

    async fn close(&mut self) -> opendal_core::Result<Metadata> {
        let len = self.copied.unwrap_or(0) as u64;
        Ok(MetadataBuilder::file(len).build())
    }

    async fn abort(&mut self) -> opendal_core::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct MemoryWithCopyService {
    inner: Servicer,
}

impl Service for MemoryWithCopyService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = CopyFromInnerCopier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info_dyn()
    }

    fn capability(&self) -> Capability {
        let mut cap = self.inner.capability_dyn();
        cap.copy = true;
        cap
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
        _args: OpCopy,
    ) -> opendal_core::Result<Self::Copier> {
        Ok(CopyFromInnerCopier {
            op: Operator::from_parts(ctx.clone(), self.inner.clone()),
            from: from.to_string(),
            to: to.to_string(),
            copied: None,
        })
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
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal_core::Result<RpRename> {
        self.inner.rename_dyn(ctx, from, to, args).await
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

fn memory_without_stat_dir_simulation() -> Result<Operator> {
    let raw_mem = Arc::new(opendal_core::Builder::build(
        opendal::services::Memory::default(),
    )?) as Servicer;
    let service = Arc::new(MemoryWithCopyService { inner: raw_mem }) as Servicer;
    Ok(Operator::from_parts(OperationContext::default(), service)
        .layer(opendal_core::layers::SimulateLayer::default().with_stat_dir(false)))
}

fn propfind_hrefs(xml: &[u8]) -> Result<Vec<String>> {
    let text = std::str::from_utf8(xml)?;
    let doc = roxmltree::Document::parse(text)?;
    Ok(doc
        .descendants()
        .filter(|node| node.tag_name().name() == "href")
        .filter_map(|node| node.text().map(ToOwned::to_owned))
        .collect())
}

#[tokio::test]
async fn test_implicit_directories_propfind_copy_delete_without_stat_dir_simulation() -> Result<()>
{
    let op = memory_without_stat_dir_simulation()?;
    op.write("a/b/c.txt", "hello").await?;
    op.write("foo", "file").await?;

    // Confirm the backend genuinely reports NotFound when stat-ing implicit
    // directory prefixes so this test exercises OpendalFs's fallback path.
    assert!(matches!(
        op.stat("a/").await,
        Err(err) if err.kind() == ErrorKind::NotFound
    ));
    assert!(matches!(
        op.stat("a/b/").await,
        Err(err) if err.kind() == ErrorKind::NotFound
    ));

    let webdavfs = OpendalFs::new(op.clone());
    assert!(webdavfs.metadata(&DavPath::new("/")?).await?.is_dir());
    for dir in ["/a", "/a/", "/a/b", "/a/b/"] {
        let meta = webdavfs.metadata(&DavPath::new(dir)?).await?;
        assert!(meta.is_dir(), "{dir} should be detected as a directory");
    }
    let file_meta = webdavfs.metadata(&DavPath::new("/a/b/c.txt")?).await?;
    assert!(file_meta.is_file());
    assert_eq!(file_meta.len(), 5);

    let foo_meta = webdavfs.metadata(&DavPath::new("/foo")?).await?;
    assert!(foo_meta.is_file());
    assert!(matches!(
        webdavfs.metadata(&DavPath::new("/foo/")?).await,
        Err(FsError::NotFound)
    ));
    for missing in ["/missing", "/missing/", "/a/missing/"] {
        assert!(
            matches!(
                webdavfs.metadata(&DavPath::new(missing)?).await,
                Err(FsError::NotFound)
            ),
            "{missing} should be NotFound"
        );
    }

    let dav = DavHandler::builder()
        .filesystem(OpendalFs::new(op.clone()))
        .build_handler();

    // PROPFIND / (Depth: 1) must include the implicit directory /a/ instead of
    // dropping it when OpendalDirEntry::metadata() stats "a/".
    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method("PROPFIND")
            .uri("/")
            .header("Depth", "1")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::MULTI_STATUS);
    let hrefs = propfind_hrefs(&body)?;
    assert!(hrefs.iter().any(|h| h == "/a/"), "missing /a/ in {hrefs:?}");
    assert!(
        hrefs.iter().any(|h| h == "/foo"),
        "missing /foo in {hrefs:?}"
    );

    // PROPFIND /a/ (Depth: 1) must list the nested implicit directory /a/b/.
    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method("PROPFIND")
            .uri("/a/")
            .header("Depth", "1")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::MULTI_STATUS);
    let hrefs = propfind_hrefs(&body)?;
    assert!(
        hrefs.iter().any(|h| h == "/a/b/"),
        "missing /a/b/ in {hrefs:?}"
    );

    // PROPFIND /a/b/ (Depth: 0) must report a collection; missing must be 404.
    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method("PROPFIND")
            .uri("/a/b/")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::MULTI_STATUS);
    assert!(propfind_is_collection(&body)?);

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("PROPFIND")
            .uri("/missing/")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // MKCOL onto an implicit directory reports 405; MKCOL under it succeeds.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("MKCOL")
            .uri("/a/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("MKCOL")
            .uri("/a/sibling/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    // Recursive COPY of an implicit directory must copy nested entries.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("COPY")
            .uri("/a/")
            .header("Destination", "/copied/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method(Method::GET)
            .uri("/copied/b/c.txt")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"hello");

    // Recursive DELETE of an implicit directory must remove its entire subtree.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method(Method::DELETE)
            .uri("/a/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("PROPFIND")
            .uri("/a/")
            .header("Depth", "0")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(!op.exists("a/b/c.txt").await?);
    Ok(())
}

#[tokio::test]
async fn test_file_move_and_copy_to_trailing_slash_destination() -> Result<()> {
    let root = tempfile::tempdir()?;
    let op = Operator::new(Fs::default().root(root.path().to_str().unwrap()))?;
    let dav = DavHandler::builder()
        .filesystem(OpendalFs::new(op.clone()))
        .build_handler();

    // MOVE file onto an existing collection with trailing slash and Overwrite: T.
    op.write("src.txt", "moved-data").await?;
    op.create_dir("dst/").await?;
    op.write("dst/old.txt", "old").await?;

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("MOVE")
            .uri("/src.txt")
            .header("Destination", "/dst/")
            .header("Overwrite", "T")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method(Method::GET)
            .uri("/dst")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"moved-data");
    assert!(!op.exists("src.txt").await?);
    assert!(root.path().join("dst").is_file());
    assert!(!root.path().join("dst/old.txt").exists());

    // COPY file onto an existing collection with trailing slash and Overwrite: T.
    op.write("src2.txt", "copied-data").await?;
    op.create_dir("copy_dst/").await?;
    op.write("copy_dst/old.txt", "old").await?;

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("COPY")
            .uri("/src2.txt")
            .header("Destination", "/copy_dst/")
            .header("Overwrite", "T")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = dav_request(
        &dav,
        Request::builder()
            .method(Method::GET)
            .uri("/copy_dst")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"copied-data");
    assert_eq!(
        op.read("src2.txt").await?.to_bytes().as_ref(),
        b"copied-data"
    );

    // COPY and MOVE to a non-existent destination with trailing slash.
    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("COPY")
            .uri("/src2.txt")
            .header("Destination", "/fresh_copy/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        op.read("fresh_copy").await?.to_bytes().as_ref(),
        b"copied-data"
    );

    let (status, _) = dav_request(
        &dav,
        Request::builder()
            .method("MOVE")
            .uri("/src2.txt")
            .header("Destination", "/fresh_move/")
            .body(Body::empty())?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        op.read("fresh_move").await?.to_bytes().as_ref(),
        b"copied-data"
    );
    assert!(!op.exists("src2.txt").await?);

    // Root "/" must be rejected as source or destination for file rename/copy.
    let webdavfs = OpendalFs::new(op);
    assert!(matches!(
        webdavfs
            .rename(&DavPath::new("/fresh_move")?, &DavPath::new("/")?)
            .await,
        Err(FsError::Forbidden)
    ));
    assert!(matches!(
        webdavfs
            .copy(&DavPath::new("/fresh_move")?, &DavPath::new("/")?)
            .await,
        Err(FsError::Forbidden)
    ));
    assert!(matches!(
        webdavfs
            .copy(&DavPath::new("/")?, &DavPath::new("/out")?)
            .await,
        Err(FsError::Forbidden)
    ));
    Ok(())
}

#[tokio::test]
async fn test_collection_rename_fallback_rejects_existing_file_and_implicit_dir() -> Result<()> {
    let op = Operator::new(opendal::services::Memory::default())?;
    op.create_dir("dir/").await?;
    op.write("file", "keep").await?;
    let webdavfs = OpendalFs::new(op.clone());

    for dst in ["/file", "/file/"] {
        assert!(
            matches!(
                webdavfs
                    .rename(&DavPath::new("/dir/")?, &DavPath::new(dst)?)
                    .await,
                Err(FsError::Exists)
            ),
            "renaming /dir/ onto existing file via {dst} must fail with Exists"
        );
    }

    assert_eq!(op.read("file").await?.to_bytes().as_ref(), b"keep");
    assert!(!op.exists("file/").await?);
    assert!(op.exists("dir/").await?);

    // Also reject renaming an empty collection onto an implicit directory when
    // stat_dir simulation is disabled.
    let implicit_op = memory_without_stat_dir_simulation()?;
    implicit_op.create_dir("empty/").await?;
    implicit_op.write("implicit/child.txt", "keep").await?;
    let implicit_fs = OpendalFs::new(implicit_op.clone());

    assert!(matches!(
        implicit_fs
            .rename(&DavPath::new("/empty/")?, &DavPath::new("/implicit/")?)
            .await,
        Err(FsError::Exists)
    ));
    assert!(implicit_op.exists("empty/").await?);
    assert_eq!(
        implicit_op
            .read("implicit/child.txt")
            .await?
            .to_bytes()
            .as_ref(),
        b"keep"
    );
    Ok(())
}

#[tokio::test]
async fn test_autoindex_directory_name_has_single_slash() -> Result<()> {
    let memory = Operator::new(opendal::services::Memory::default())?;
    let root = tempfile::tempdir()?;
    let fs_op = Operator::new(Fs::default().root(root.path().to_str().unwrap()))?;

    for (label, op) in [("memory", memory), ("fs", fs_op)] {
        op.create_dir("sub/").await?;
        op.write("sub/file.txt", "hi").await?;

        let webdavfs = OpendalFs::new(op.clone());
        let entries: Vec<String> = webdavfs
            .read_dir(&DavPath::new("/")?, ReadDirMeta::None)
            .await?
            .map(|entry| String::from_utf8(entry.unwrap().name()).unwrap())
            .collect()
            .await;
        assert_eq!(entries, vec!["sub".to_string()], "{label}: bare entry name");

        let dav = DavHandler::builder()
            .filesystem(OpendalFs::new(op))
            .autoindex(true)
            .build_handler();
        let (status, body) = dav_request(
            &dav,
            Request::builder()
                .method(Method::GET)
                .uri("/")
                .body(Body::empty())?,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{label}: GET / autoindex");
        let html = std::str::from_utf8(&body)?;
        assert!(
            html.contains("sub/"),
            "{label}: expected single-slash sub/ in {html}"
        );
        assert!(
            !html.contains("sub//"),
            "{label}: unexpected double-slash sub// in {html}"
        );
    }
    Ok(())
}
