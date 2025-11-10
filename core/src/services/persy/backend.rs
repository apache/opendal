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

use std::fmt::Debug;
use std::sync::Arc;

use super::PERSY_SCHEME;
use super::config::PersyConfig;
use super::core::*;
use super::deleter::PersyDeleter;
use super::writer::PersyWriter;
use crate::raw::*;
use crate::*;

/// persy service support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct PersyBuilder {
    pub(super) config: PersyConfig,
}

impl PersyBuilder {
    /// Set the path to the persy data directory. Will create if not exists.
    pub fn datafile(mut self, path: &str) -> Self {
        self.config.datafile = Some(path.into());
        self
    }

    /// Set the name of the persy segment. Will create if not exists.
    pub fn segment(mut self, path: &str) -> Self {
        self.config.segment = Some(path.into());
        self
    }

    /// Set the name of the persy index. Will create if not exists.
    pub fn index(mut self, path: &str) -> Self {
        self.config.index = Some(path.into());
        self
    }
}

impl Builder for PersyBuilder {
    type Config = PersyConfig;

    fn build(self) -> Result<impl Access> {
        let datafile_path = self.config.datafile.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datafile is required but not set")
                .with_context("service", PERSY_SCHEME)
        })?;

        let segment_name = self.config.segment.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "segment is required but not set")
                .with_context("service", PERSY_SCHEME)
        })?;

        let segment = segment_name.clone();

        let index_name = self.config.index.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "index is required but not set")
                .with_context("service", PERSY_SCHEME)
        })?;

        let index = index_name.clone();

        let persy = persy::OpenOptions::new()
            .create(true)
            .prepare_with(move |p| init(p, &segment_name, &index_name))
            .open(&datafile_path)
            .map_err(|e| {
                Error::new(ErrorKind::ConfigInvalid, "open db")
                    .with_context("service", PERSY_SCHEME)
                    .with_context("datafile", datafile_path.clone())
                    .set_source(e)
            })?;

        // This function will only be called on database creation
        fn init(
            persy: &persy::Persy,
            segment_name: &str,
            index_name: &str,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let mut tx = persy.begin()?;

            if !tx.exists_segment(segment_name)? {
                tx.create_segment(segment_name)?;
            }
            if !tx.exists_index(index_name)? {
                tx.create_index::<String, persy::PersyId>(index_name, persy::ValueMode::Replace)?;
            }

            let prepared = tx.prepare()?;
            prepared.commit()?;

            Ok(())
        }

        Ok(PersyBackend::new(PersyCore {
            datafile: datafile_path,
            segment,
            index,
            persy,
        }))
    }
}

/// Backend for persy services.
#[derive(Clone, Debug)]
pub struct PersyBackend {
    core: Arc<PersyCore>,
    root: String,
    info: Arc<AccessorInfo>,
}

impl PersyBackend {
    pub fn new(core: PersyCore) -> Self {
        let info = AccessorInfo::default();
        info.set_scheme(PERSY_SCHEME);
        info.set_name(&core.datafile);
        info.set_root("/");
        info.set_native_capability(Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            shared: false,
            ..Default::default()
        });

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info: Arc::new(info),
        }
    }
}

impl Access for PersyBackend {
    type Reader = Buffer;
    type Writer = PersyWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<PersyDeleter>;

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_abs_path(&self.root, path);

        if p == build_abs_path(&self.root, "") {
            Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
        } else {
            let bs = self.core.get(&p)?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    Metadata::new(EntryMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in persy")),
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let p = build_abs_path(&self.root, path);
        let bs = match self.core.get(&p)? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(ErrorKind::NotFound, "kv not found in persy"));
            }
        };
        Ok((RpRead::new(), bs.slice(args.range().to_range_as_usize())))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let p = build_abs_path(&self.root, path);
        Ok((RpWrite::new(), PersyWriter::new(self.core.clone(), p)))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(PersyDeleter::new(self.core.clone(), self.root.clone())),
        ))
    }
}
