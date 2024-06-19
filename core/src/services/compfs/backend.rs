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

use super::{core::CompfsCore, lister::CompfsLister, reader::CompfsReader, writer::CompfsWriter};
use crate::raw::*;
use crate::*;

use std::{collections::HashMap, io::Cursor, path::PathBuf, sync::Arc};

/// [`compio`]-based file system support.
#[derive(Debug, Clone, Default)]
pub struct CompfsBuilder {
    root: Option<PathBuf>,
}

impl CompfsBuilder {
    /// Set root for Compfs
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(PathBuf::from(root))
        };

        self
    }
}

impl Builder for CompfsBuilder {
    const SCHEME: Scheme = Scheme::Compfs;
    type Accessor = ();

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = CompfsBuilder::default();

        map.get("root").map(|v| builder.root(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        todo!()
    }
}

#[derive(Debug)]
pub struct CompfsBackend {
    core: Arc<CompfsCore>,
}

impl Access for CompfsBackend {
    type Reader = CompfsReader;
    type Writer = CompfsWriter;
    type Lister = Option<CompfsLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Compfs)
            .set_root(&self.core.root.to_string_lossy())
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,
                write_can_empty: true,
                write_can_multi: true,
                create_dir: true,
                delete: true,

                list: true,

                copy: true,
                rename: true,
                blocking: true,

                ..Default::default()
            });

        am
    }

    async fn read(&self, path: &str, op: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = self.core.root.join(path.trim_end_matches('/'));

        let file = self
            .core
            .exec(|| async move { compio::fs::OpenOptions::new().read(true).open(&path).await })
            .await?;

        let r = CompfsReader::new(self.core.clone(), file, op.range());
        Ok((RpRead::new(), r))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let path = self.core.root.join(path.trim_end_matches('/'));
        let append = args.append();
        let file = self
            .core
            .exec(move || async move {
                compio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(!append)
                    .open(path)
                    .await
            })
            .await
            .map(Cursor::new)?;

        let w = CompfsWriter::new(self.core.clone(), file);
        Ok((RpWrite::new(), w))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let path = self.core.root.join(path.trim_end_matches('/'));
        let read_dir = match self
            .core
            .exec_blocking(move || std::fs::read_dir(path))
            .await?
        {
            Ok(rd) => rd,
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::NotFound {
                    Ok((RpList::default(), None))
                } else {
                    Err(new_std_io_error(e))
                };
            }
        };

        let lister = CompfsLister::new(self.core.clone(), read_dir);
        Ok((RpList::default(), Some(lister)))
    }
}
