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

use std::sync::Arc;

use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::File;
use web_sys::FileSystemWritableFileStream;

use super::OPFS_SCHEME;
use super::config::OpfsConfig;
use super::error::*;
use super::lister::OpfsLister;
use super::reader::OpfsReader;
use super::utils::*;
use super::writer::OpfsWriter;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct OpfsBuilder {
    pub(super) config: OpfsConfig,
}

impl OpfsBuilder {
    /// Set root directory for this backend.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }
}

impl Builder for OpfsBuilder {
    type Config = OpfsConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        Ok(OpfsBackend { root })
    }
}

/// OPFS Service backend
#[derive(Debug, Clone)]
pub struct OpfsBackend {
    root: String,
}

impl Access for OpfsBackend {
    type Reader = OpfsReader;

    type Writer = OpfsWriter;

    type Lister = OpfsLister;

    type Deleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_scheme(OPFS_SCHEME);
        info.set_name("opfs");
        info.set_root(&self.root);
        info.set_native_capability(Capability {
            stat: true,

            read: true,

            list: true,

            create_dir: true,

            write: true,
            write_can_empty: true,
            write_can_multi: true,

            ..Default::default()
        });
        Arc::new(info)
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p.ends_with('/') {
            get_directory_handle(&p, false).await?;
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        // File: get metadata via getFile().
        let handle = get_file_handle(&p, false).await?;
        let file: File = JsFuture::from(handle.get_file())
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;

        let mut meta = Metadata::new(EntryMode::FILE);
        meta.set_content_length(file.size() as u64);
        if let Ok(t) = Timestamp::from_millisecond(file.last_modified() as i64) {
            meta.set_last_modified(t);
        }

        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let handle = get_file_handle(&p, false).await?;

        Ok((RpRead::new(), OpfsReader::new(handle, args.range())))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let p = build_abs_path(&self.root, path);
        let dir = get_directory_handle(&p, false).await?;

        Ok((RpList::default(), OpfsLister::new(dir, path.to_string())))
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        debug_assert!(path != "/", "root path should be handled upstream");
        let p = build_abs_path(&self.root, path);
        get_directory_handle(&p, true).await?;

        Ok(RpCreateDir::default())
    }

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        let handle = get_file_handle(&p, true).await?;
        console_debug!("write: handle = {:?}", handle);
        console_debug!("write: path   = {:?}", p);
        let stream: FileSystemWritableFileStream = JsFuture::from(handle.create_writable())
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;

        Ok((RpWrite::default(), OpfsWriter::new(stream)))
    }
}
