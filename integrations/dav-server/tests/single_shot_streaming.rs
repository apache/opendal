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

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dav_server::davpath::DavPath;
use dav_server::fs::{DavFileSystem, OpenOptions};
use dav_server_opendalfs::OpendalFs;
use opendal::raw::oio;
use opendal::raw::{Access, AccessorInfo, OpWrite, RpWrite};
use opendal::{Buffer, Capability, Metadata, Operator};

#[derive(Clone)]
struct SingleShotAccess {
    info: Arc<AccessorInfo>,
    stored: Arc<Mutex<Vec<u8>>>,
}

impl Debug for SingleShotAccess {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleShotAccess").finish()
    }
}

impl SingleShotAccess {
    fn new(stored: Arc<Mutex<Vec<u8>>>) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme("memory")
            .set_root("/")
            .set_name("single-shot-probe")
            .set_native_capability(Capability {
                write: true,
                ..Default::default()
            });
        Self {
            info: Arc::new(info),
            stored,
        }
    }
}

impl Access for SingleShotAccess {
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
            Box::new(oio::OneShotWriter::new(Store(self.stored.clone()))),
        ))
    }
}

struct Store(Arc<Mutex<Vec<u8>>>);

impl oio::OneShotWrite for Store {
    async fn write_once(&self, buffer: Buffer) -> opendal::Result<Metadata> {
        let size = buffer.len() as u64;
        *self.0.lock().unwrap() = buffer.to_bytes().to_vec();
        Ok(Metadata::new(opendal::EntryMode::FILE).with_content_length(size))
    }
}

#[tokio::test]
async fn small_multi_frame_write_bytes_works_with_single_shot_writer() {
    let stored = Arc::new(Mutex::new(Vec::new()));
    let op = Operator::from_inner(Arc::new(SingleShotAccess::new(stored.clone())));
    let fs = OpendalFs::new(op);
    let mut file = fs
        .open(
            &DavPath::new("/small-upload").unwrap(),
            OpenOptions {
                write: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    file.write_bytes(Bytes::from_static(b"hello"))
        .await
        .unwrap();
    file.write_bytes(Bytes::from_static(b"world"))
        .await
        .unwrap();
    file.flush().await.unwrap();
    assert_eq!(stored.lock().unwrap().as_slice(), b"helloworld");
}

#[tokio::test]
async fn small_multi_frame_write_buf_works_with_single_shot_writer() {
    let stored = Arc::new(Mutex::new(Vec::new()));
    let op = Operator::from_inner(Arc::new(SingleShotAccess::new(stored.clone())));
    let fs = OpendalFs::new(op);
    let mut file = fs
        .open(
            &DavPath::new("/small-upload").unwrap(),
            OpenOptions {
                write: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    file.write_buf(Box::new(Bytes::from_static(b"hello")))
        .await
        .unwrap();
    file.write_buf(Box::new(Bytes::from_static(b"world")))
        .await
        .unwrap();
    file.flush().await.unwrap();
    assert_eq!(stored.lock().unwrap().as_slice(), b"helloworld");
}
