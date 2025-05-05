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
use std::fmt::Formatter;
use std::str;

use cacache;

use crate::raw::adapters::kv;
use crate::raw::Access;
use crate::services::CacacheConfig;
use crate::Builder;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;
use crate::*;

impl Configurator for CacacheConfig {
    type Builder = CacacheBuilder;
    fn into_builder(self) -> Self::Builder {
        CacacheBuilder { config: self }
    }
}

/// cacache service support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct CacacheBuilder {
    config: CacacheConfig,
}

impl CacacheBuilder {
    /// Set the path to the cacache data directory. Will create if not exists.
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }
}

impl Builder for CacacheBuilder {
    const SCHEME: Scheme = Scheme::Cacache;
    type Config = CacacheConfig;

    fn build(self) -> Result<impl Access> {
        let datadir_path = self.config.datadir.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", Scheme::Cacache)
        })?;

        Ok(CacacheBackend::new(Adapter {
            datadir: datadir_path,
        }))
    }
}

/// Backend for cacache services.
pub type CacacheBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    datadir: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.datadir);
        ds.finish()
    }
}

impl kv::Adapter for Adapter {
    type Scanner = ();

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Cacache,
            &self.datadir,
            Capability {
                read: true,
                write: true,
                delete: true,
                blocking: true,
                shared: false,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let result = cacache::read(&self.datadir, path)
            .await
            .map_err(parse_error)?;
        Ok(Some(Buffer::from(result)))
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Buffer>> {
        let result = cacache::read_sync(&self.datadir, path).map_err(parse_error)?;
        Ok(Some(Buffer::from(result)))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        cacache::write(&self.datadir, path, value.to_vec())
            .await
            .map_err(parse_error)?;
        Ok(())
    }

    fn blocking_set(&self, path: &str, value: Buffer) -> Result<()> {
        cacache::write_sync(&self.datadir, path, value.to_vec()).map_err(parse_error)?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        cacache::remove(&self.datadir, path)
            .await
            .map_err(parse_error)?;

        Ok(())
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        cacache::remove_sync(&self.datadir, path).map_err(parse_error)?;

        Ok(())
    }
}

fn parse_error(err: cacache::Error) -> Error {
    let kind = match err {
        cacache::Error::EntryNotFound(_, _) => ErrorKind::NotFound,
        _ => ErrorKind::Unexpected,
    };

    Error::new(kind, "error from cacache").set_source(err)
}
