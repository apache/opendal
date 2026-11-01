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

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dav_server::davpath::DavPath;
use dav_server::fs::{DavFile, DavFileSystem, OpenOptions};
use dav_server_opendalfs::OpendalFs;
use opendal_core::raw::*;
use opendal_core::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    SingleShot,
    WriteError,
    CloseError,
}

#[derive(Debug, Default)]
struct Observed {
    writes: usize,
    closes: usize,
    aborts: usize,
    data: Vec<u8>,
}

#[derive(Debug)]
struct TestService {
    mode: Mode,
    observed: Arc<Mutex<Observed>>,
}

impl Service for TestService {
    type Reader = ();
    type Writer = oio::Writer;
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("dav-test", "/", "writer-lifecycle")
    }

    fn capability(&self) -> Capability {
        Capability {
            write: true,
            write_can_multi: self.mode != Mode::SingleShot,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(ErrorKind::Unsupported, "write-only fixture"))
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        Err(Error::new(ErrorKind::Unsupported, "write-only fixture"))
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        Err(Error::new(ErrorKind::Unsupported, "write-only fixture"))
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        Ok(match self.mode {
            Mode::SingleShot => Box::new(oio::OneShotWriter::new(Store(self.observed.clone()))),
            mode => Box::new(FailingWriter {
                mode,
                observed: self.observed.clone(),
            }),
        })
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        Err(Error::new(ErrorKind::Unsupported, "write-only fixture"))
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        Err(Error::new(ErrorKind::Unsupported, "write-only fixture"))
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Err(Error::new(ErrorKind::Unsupported, "write-only fixture"))
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(ErrorKind::Unsupported, "write-only fixture"))
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(Error::new(ErrorKind::Unsupported, "write-only fixture"))
    }
}

struct Store(Arc<Mutex<Observed>>);

impl oio::OneShotWrite for Store {
    async fn write_once(&self, buffer: Buffer) -> Result<Metadata> {
        let size = buffer.len() as u64;
        let mut observed = self.0.lock().unwrap();
        observed.writes += 1;
        observed.data = buffer.to_bytes().to_vec();
        Ok(MetadataBuilder::file(size).build())
    }
}

struct FailingWriter {
    mode: Mode,
    observed: Arc<Mutex<Observed>>,
}

impl oio::Write for FailingWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        let mut observed = self.observed.lock().unwrap();
        observed.writes += 1;
        if self.mode == Mode::WriteError {
            return Err(Error::new(ErrorKind::Unexpected, "injected write failure"));
        }
        observed.data.extend_from_slice(&buffer.to_bytes());
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let mut observed = self.observed.lock().unwrap();
        observed.closes += 1;
        if self.mode == Mode::CloseError {
            return Err(Error::new(ErrorKind::Unexpected, "injected close failure"));
        }
        Ok(MetadataBuilder::file(observed.data.len() as u64).build())
    }

    async fn abort(&mut self) -> Result<()> {
        self.observed.lock().unwrap().aborts += 1;
        Ok(())
    }
}

async fn open_file(mode: Mode) -> (Box<dyn DavFile>, Arc<Mutex<Observed>>) {
    let observed = Arc::new(Mutex::new(Observed::default()));
    let service = TestService {
        mode,
        observed: observed.clone(),
    };
    let op = Operator::from_parts(OperationContext::default(), Arc::new(service));
    let fs = OpendalFs::new(op);
    let file = fs
        .open(
            &DavPath::new("/upload").unwrap(),
            OpenOptions {
                write: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    (file, observed)
}

#[tokio::test]
async fn small_write_bytes_frames_work_with_single_shot_writer() {
    let (mut file, observed) = open_file(Mode::SingleShot).await;
    file.write_bytes(Bytes::from_static(b"hello"))
        .await
        .unwrap();
    file.write_bytes(Bytes::from_static(b"world"))
        .await
        .unwrap();
    file.flush().await.unwrap();
    let observed = observed.lock().unwrap();
    assert_eq!(observed.data, b"helloworld");
    assert_eq!(observed.writes, 1);
}

#[tokio::test]
async fn small_write_buf_frames_work_with_single_shot_writer() {
    let (mut file, observed) = open_file(Mode::SingleShot).await;
    file.write_buf(Box::new(Bytes::from_static(b"hello")))
        .await
        .unwrap();
    file.write_buf(Box::new(Bytes::from_static(b"world")))
        .await
        .unwrap();
    file.flush().await.unwrap();
    let observed = observed.lock().unwrap();
    assert_eq!(observed.data, b"helloworld");
    assert_eq!(observed.writes, 1);
}

#[tokio::test]
async fn write_bytes_failure_aborts_and_terminates_writer() {
    let (mut file, observed) = open_file(Mode::WriteError).await;
    assert!(
        file.write_bytes(Bytes::from(vec![0; 512 * 1024 + 1]))
            .await
            .is_err()
    );
    assert!(file.flush().await.is_err());
    assert!(
        file.write_bytes(Bytes::from_static(b"later"))
            .await
            .is_err()
    );
    let observed = observed.lock().unwrap();
    assert_eq!(observed.writes, 1);
    assert_eq!(observed.closes, 0);
    assert_eq!(observed.aborts, 1);
}

#[tokio::test]
async fn write_buf_failure_aborts_and_terminates_writer() {
    let (mut file, observed) = open_file(Mode::WriteError).await;
    assert!(
        file.write_buf(Box::new(Bytes::from(vec![0; 512 * 1024 + 1])))
            .await
            .is_err()
    );
    assert!(file.flush().await.is_err());
    assert!(
        file.write_buf(Box::new(Bytes::from_static(b"later")))
            .await
            .is_err()
    );
    let observed = observed.lock().unwrap();
    assert_eq!(observed.writes, 1);
    assert_eq!(observed.closes, 0);
    assert_eq!(observed.aborts, 1);
}

#[tokio::test]
async fn buffered_write_failure_during_flush_aborts_writer() {
    let (mut file, observed) = open_file(Mode::WriteError).await;
    file.write_bytes(Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert!(file.flush().await.is_err());
    assert!(file.flush().await.is_err());
    let observed = observed.lock().unwrap();
    assert_eq!(observed.writes, 1);
    assert_eq!(observed.closes, 0);
    assert_eq!(observed.aborts, 1);
}

#[tokio::test]
async fn close_failure_aborts_and_terminates_writer() {
    let (mut file, observed) = open_file(Mode::CloseError).await;
    file.write_bytes(Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert!(file.flush().await.is_err());
    assert!(file.flush().await.is_err());
    assert!(
        file.write_bytes(Bytes::from_static(b"later"))
            .await
            .is_err()
    );
    let observed = observed.lock().unwrap();
    assert_eq!(observed.writes, 1);
    assert_eq!(observed.closes, 1);
    assert_eq!(observed.aborts, 1);
}
