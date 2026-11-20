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
use dav_server::davpath::DavPath;
use dav_server::fs::{DavFile, FsError, OpenOptions};
use dav_server::fs::{DavFileSystem, ReadDirMeta};
use dav_server_opendalfs::OpendalFs;
use futures::StreamExt;
use opendal::Operator;
use opendal::services::Fs;
use opendal_core::raw::{
    OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite, RpCreateDir,
    RpPresign, RpRename, RpStat, Service, ServiceInfo, oio,
};
use opendal_core::{
    Buffer, Capability, Error, ErrorKind, Metadata, MetadataBuilder, OperationContext,
};
use std::fs;
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

async fn open_file_with_failure(failure: Failure) -> (Box<dyn DavFile>, Arc<AtomicBool>) {
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
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();
    (file, aborted)
}

#[tokio::test]
async fn test_failed_write_aborts_before_returning() {
    for write_buf in [false, true] {
        let (mut file, aborted) = open_file_with_failure(Failure::Write).await;
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
    }
}

#[tokio::test]
async fn test_failed_close_aborts_before_returning() {
    let (mut file, aborted) = open_file_with_failure(Failure::Close).await;
    file.write_bytes(Bytes::from_static(b"body")).await.unwrap();

    assert!(matches!(file.flush().await, Err(FsError::GeneralFailure)));
    assert!(
        aborted.load(Ordering::Relaxed),
        "close failure must abort the writer"
    );
}
