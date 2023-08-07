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

use std::pin::Pin;
use std::task::Poll::{Pending, Ready};

use dav_server::fs::DavDirEntry;
use dav_server::fs::DavFile;
use dav_server::fs::DavFileSystem;
use dav_server::fs::DavMetaData;
use futures::FutureExt;
use futures_util::Stream;
use futures_util::StreamExt;
use opendal::Lister;
use opendal::Operator;

use crate::services::webdav::webdav_dir_entry::WebDAVDirEntry;

use super::webdav_file::WebdavFile;
use super::webdav_metadata::WebdavMetaData;

#[derive(Clone)]
pub struct WebdavFs {
    pub op: Operator,
}

impl DavFileSystem for WebdavFs {
    fn open<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        options: dav_server::fs::OpenOptions,
    ) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavFile>> {
        async move {
            let file = WebdavFile {
                op: self.op.clone(),
                path: path.clone(),
                options,
            };
            Ok(Box::new(file) as Box<dyn DavFile>)
        }
        .boxed()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        _meta: dav_server::fs::ReadDirMeta,
    ) -> dav_server::fs::FsFuture<dav_server::fs::FsStream<Box<dyn dav_server::fs::DavDirEntry>>>
    {
        async move {
            let lister = self.op.lister(path.as_url_string().as_str()).await.unwrap();
            Ok(DavStream::new(self.op.clone(), lister).boxed())
        }
        .boxed()
    }

    fn metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavMetaData>> {
        async move {
            let opendal_metadata = self.op.stat(path.as_url_string().as_str()).await.unwrap();
            Ok(Box::new(WebdavMetaData::new(opendal_metadata)) as Box<dyn DavMetaData>)
        }
        .boxed()
    }
}

impl WebdavFs {
    pub fn new(op: Operator) -> Box<WebdavFs> {
        Box::new(WebdavFs { op })
    }
}

struct DavStream {
    op: Operator,
    lister: Lister,
}

impl Stream for DavStream {
    type Item = Box<dyn DavDirEntry>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let dav_stream = self.get_mut();
        let lister = Pin::new(&mut dav_stream.lister).get_mut();

        match Pin::new(lister).poll_next(cx) {
            Ready(entry) => match entry {
                Some(entry) => {
                    let webdav_entry = WebDAVDirEntry::new(entry.unwrap(), dav_stream.op.clone());
                    Ready(Some(Box::new(webdav_entry) as Box<dyn DavDirEntry>))
                }
                None => Ready(None),
            },
            Pending => Pending,
        }
    }
}

impl DavStream {
    fn new(op: Operator, lister: Lister) -> Self {
        DavStream { op, lister }
    }
}
