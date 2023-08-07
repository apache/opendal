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

use std::io::SeekFrom;

use bytes::Bytes;
use dav_server::davpath::DavPath;
use dav_server::fs::DavFile;
use dav_server::fs::DavMetaData;
use dav_server::fs::FsFuture;
use dav_server::fs::OpenOptions;
use futures::FutureExt;
use opendal::Operator;

use super::webdav_metadata::WebdavMetaData;

#[derive(Debug)]
pub struct WebdavFile {
    pub op: Operator,
    pub path: DavPath,
    pub options: OpenOptions,
}

impl DavFile for WebdavFile {
    fn read_bytes(&mut self, count: usize) -> FsFuture<Bytes> {
        async move {
            let file_path = self.path.as_url_string();
            let content = self
                .op
                .range_read(&file_path, 0..count as u64)
                .await
                .unwrap();
            //error handle ?
            Ok(Bytes::from(content))
        }
        .boxed()
    }

    fn metadata(&mut self) -> FsFuture<Box<dyn DavMetaData>> {
        async move {
            let opendal_metadata = self
                .op
                .stat(self.path.as_url_string().as_str())
                .await
                .unwrap();
            Ok(Box::new(WebdavMetaData::new(opendal_metadata)) as Box<dyn DavMetaData>)
        }
        .boxed()
    }

    fn write_buf(&mut self, buf: Box<dyn bytes::Buf + Send>) -> FsFuture<()> {
        self.write_bytes(Bytes::copy_from_slice(buf.chunk()))
    }

    fn write_bytes(&mut self, buf: Bytes) -> FsFuture<()> {
        async move {
            let file_path = self.path.as_url_string();
            self.op.write(&file_path, buf).await.map_err(convert_error)
        }
        .boxed()
    }

    fn seek(&mut self, _pos: SeekFrom) -> FsFuture<u64> {
        futures_util::future::err(dav_server::fs::FsError::NotImplemented).boxed()
    }

    fn flush(&mut self) -> FsFuture<()> {
        futures_util::future::ok(()).boxed()
    }
}

fn convert_error(opendal_error: opendal::Error) -> dav_server::fs::FsError {
    match opendal_error.kind() {
        opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::IsSameFile => {
            dav_server::fs::FsError::Exists
        }
        opendal::ErrorKind::NotFound => dav_server::fs::FsError::NotFound,
        _ => dav_server::fs::FsError::GeneralFailure,
    }
}
