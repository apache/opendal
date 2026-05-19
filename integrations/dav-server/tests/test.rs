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
use dav_server::fs::OpenOptions;
use dav_server::fs::{DavFileSystem, ReadDirMeta};
use dav_server_opendalfs::OpendalFs;
use futures::StreamExt;
use opendal::Buffer;
use opendal::Operator;
use opendal::raw::oio;
use opendal::raw::{Access, AccessorInfo, OpWrite, RpWrite};
use opendal::services::Fs;
use opendal::{Capability, Error, ErrorKind, Metadata};
use std::fmt::{Debug, Formatter};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn test() -> Result<()> {
    let builder = Fs::default().root("/tmp");

    let op = Operator::new(builder)?.finish();

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
    let op = Operator::new(builder).unwrap().finish();
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
    let op = Operator::new(builder).unwrap().finish();
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

#[derive(Clone)]
struct AbortTrackingAccess {
    info: Arc<AccessorInfo>,
    aborted: Arc<AtomicBool>,
    fail_on_write: bool,
    fail_on_close: bool,
}

impl Debug for AbortTrackingAccess {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AbortTrackingAccess").finish()
    }
}

impl AbortTrackingAccess {
    fn with_failures(aborted: Arc<AtomicBool>, fail_on_write: bool, fail_on_close: bool) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme("memory")
            .set_root("/")
            .set_name("abort-tracking")
            .set_native_capability(Capability {
                write: true,
                ..Default::default()
            });

        Self {
            info: info.into(),
            aborted,
            fail_on_write,
            fail_on_close,
        }
    }

    fn write_failure(aborted: Arc<AtomicBool>) -> Self {
        Self::with_failures(aborted, true, false)
    }

    fn close_failure(aborted: Arc<AtomicBool>) -> Self {
        Self::with_failures(aborted, false, true)
    }
}

impl Access for AbortTrackingAccess {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn write(&self, _: &str, _: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::new(),
            Box::new(AbortTrackingWriter {
                aborted: self.aborted.clone(),
                fail_on_write: self.fail_on_write,
                fail_on_close: self.fail_on_close,
            }),
        ))
    }
}

struct AbortTrackingWriter {
    aborted: Arc<AtomicBool>,
    fail_on_write: bool,
    fail_on_close: bool,
}

impl oio::Write for AbortTrackingWriter {
    async fn write(&mut self, _: Buffer) -> opendal::Result<()> {
        if self.fail_on_write {
            return Err(Error::new(ErrorKind::Unexpected, "injected write failure"));
        }

        Ok(())
    }

    async fn close(&mut self) -> opendal::Result<Metadata> {
        if self.fail_on_close {
            return Err(Error::new(ErrorKind::Unexpected, "injected close failure"));
        }

        Ok(Metadata::default())
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        self.aborted.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_failed_write_aborts_before_drop() {
    let aborted = Arc::new(AtomicBool::new(false));
    let op = Operator::from_inner(Arc::new(AbortTrackingAccess::write_failure(
        aborted.clone(),
    )));
    let webdavfs = OpendalFs::new(op);

    let mut file = webdavfs
        .open(
            &DavPath::new("/failed-write").unwrap(),
            OpenOptions {
                write: true,
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();

    let err = file.write_bytes(Bytes::from(vec![1; 300 * 1024])).await;
    assert!(err.is_err());

    drop(file);

    assert!(
        aborted.load(Ordering::SeqCst),
        "writer.abort() should be called when a write fails before close()"
    );
}

#[tokio::test]
async fn test_failed_close_aborts_before_drop() {
    let aborted = Arc::new(AtomicBool::new(false));
    let op = Operator::from_inner(Arc::new(AbortTrackingAccess::close_failure(
        aborted.clone(),
    )));
    let webdavfs = OpendalFs::new(op);

    let mut file = webdavfs
        .open(
            &DavPath::new("/failed-close").unwrap(),
            OpenOptions {
                write: true,
                ..OpenOptions::default()
            },
        )
        .await
        .unwrap();

    file.write_bytes(Bytes::from(vec![1; 300 * 1024]))
        .await
        .unwrap();

    let err = file.flush().await;
    assert!(err.is_err());

    drop(file);

    assert!(
        aborted.load(Ordering::SeqCst),
        "writer.abort() should be called when close() fails during flush()"
    );
}
