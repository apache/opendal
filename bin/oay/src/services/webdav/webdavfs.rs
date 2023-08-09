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

use std::path::Path;
use std::pin::Pin;
use std::task::Poll::{Pending, Ready};

use dav_server::davpath::DavPath;
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

use super::webdav_file::{convert_error, WebdavFile};
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
            let opendal_metadata = self.op.stat(path.as_url_string().as_str()).await;
            match opendal_metadata {
                Ok(metadata) => {
                    let webdav_metadata = WebdavMetaData::new(metadata);
                    Ok(Box::new(webdav_metadata) as Box<dyn DavMetaData>)
                }
                Err(e) => Err(convert_error(e)),
            }
        }
        .boxed()
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> dav_server::fs::FsFuture<()> {
        async move {
            let path = path.as_url_string();

            // check if the parent path is exist.
            // During MKCOL processing, a server MUST make the Request-URI a member of its parent collection, unless the Request-URI is "/".  If no such ancestor exists, the method MUST fail.
            // refer to https://datatracker.ietf.org/doc/html/rfc2518#section-8.3.1
            let parent = Path::new(&path).parent().unwrap();
            match self.op.is_exist(parent.to_str().unwrap()).await {
                Ok(exist) => {
                    if !exist && parent != Path::new("/") {
                        return Err(dav_server::fs::FsError::NotFound);
                    }
                }
                Err(e) => {
                    return Err(convert_error(e));
                }
            }

            let path = path.as_str();
            // check if the given path is exist (MKCOL on existing collection should fail (RFC2518:8.3.1))
            let exist = self.op.is_exist(path).await;
            match exist {
                Ok(exist) => match exist {
                    true => Err(dav_server::fs::FsError::Exists),
                    false => {
                        let res = self.op.create_dir(path).await;
                        match res {
                            Ok(_) => Ok(()),
                            Err(e) => Err(convert_error(e)),
                        }
                    }
                },
                Err(e) => Err(convert_error(e)),
            }
        }
        .boxed()
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> dav_server::fs::FsFuture<()> {
        async move {
            self.op
                .delete(path.as_url_string().as_str())
                .await
                .map_err(convert_error)
        }
        .boxed()
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> dav_server::fs::FsFuture<()> {
        self.remove_file(path)
    }

    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> dav_server::fs::FsFuture<()> {
        async move {
            self.op
                .copy(&from.as_url_string(), &to.as_url_string())
                .await
                .map_err(convert_error)
        }
        .boxed()
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> dav_server::fs::FsFuture<()> {
        async move {
            self.op
                .rename(from.as_url_string().as_str(), to.as_url_string().as_str())
                .await
                .map_err(convert_error)
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
