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

use dav_server::fs::DavMetaData;
use dav_server::fs::FsError;
use opendal::Metadata;

#[derive(Debug, Clone)]
pub struct WebdavMetaData {
    metadata: Metadata,
}

impl WebdavMetaData {
    pub fn new(metadata: Metadata) -> Self {
        WebdavMetaData { metadata }
    }
}

impl DavMetaData for WebdavMetaData {
    fn len(&self) -> u64 {
        self.metadata.content_length()
    }

    fn modified(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        match self.metadata.last_modified() {
            Some(t) => Ok(t.into()),
            None => Err(FsError::GeneralFailure),
        }
    }

    fn is_dir(&self) -> bool {
        self.metadata.is_dir()
    }

    fn is_file(&self) -> bool {
        self.metadata.is_file()
    }

    fn etag(&self) -> Option<String> {
        self.metadata.etag().map(|s| s.to_string())
    }

    fn status_changed(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        self.metadata
            .last_modified()
            .map_or(Err(FsError::GeneralFailure), |t| Ok(t.into()))
    }
}
