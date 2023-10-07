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

use dav_server::fs::DavDirEntry;
use futures::FutureExt;
use opendal::Entry;
use opendal::Operator;

use super::file::convert_error;
use super::metadata::WebdavMetaData;

pub struct WebDAVDirEntry {
    dir_entry: Entry,
    op: Operator,
}

impl DavDirEntry for WebDAVDirEntry {
    fn name(&self) -> Vec<u8> {
        self.dir_entry.name().as_bytes().to_vec()
    }

    fn metadata(&self) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavMetaData>> {
        async move {
            self.op
                .stat(self.dir_entry.path())
                .await
                .map(|metadata| {
                    Box::new(WebdavMetaData::new(metadata)) as Box<dyn dav_server::fs::DavMetaData>
                })
                .map_err(convert_error)
        }
        .boxed()
    }
}

impl WebDAVDirEntry {
    pub fn new(dir_entry: Entry, op: Operator) -> Self {
        WebDAVDirEntry { dir_entry, op }
    }
}
