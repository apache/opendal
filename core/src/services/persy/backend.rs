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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str;

use async_trait::async_trait;
use persy;

use crate::raw::adapters::kv;
use crate::Builder;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;
use crate::*;

/// persy service support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct PersyBuilder {
    /// That path to the persy data file.
    datafile: Option<String>,
}

impl PersyBuilder {
    /// Set the path to the persy data directory. Will create if not exists.
    pub fn datafile(&mut self, path: &str) -> &mut Self {
        self.datafile = Some(path.into());
        self
    }
}

impl Builder for PersyBuilder {
    const SCHEME: Scheme = Scheme::Persy;
    type Accessor = PersyBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = PersyBuilder::default();

        map.get("datafile").map(|v| builder.datafile(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let datafile_path = self.datafile.take().ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datafile is required but not set")
                .with_context("service", Scheme::Persy)
        })?;

        let persy = persy::Persy::open(&datafile_path, persy::Config::new()).map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "open db")
                .with_context("service", Scheme::Persy)
                .with_context("datafile", datafile_path.clone())
                .set_source(e)
        })?;

        Ok(PersyBackend::new(Adapter {
            datafile: datafile_path,
            persy,
        }))
    }
}

/// Backend for persy services.
pub type PersyBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    datafile: String,
    persy: persy::Persy,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.datafile);
        ds.finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Persy,
            &self.datafile,
            Capability {
                read: true,
                write: true,
                delete: true,
                blocking: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        match self.persy.scan(path) {
            Ok(bs) => {
                let mut value = vec![];
                for (_id, content) in bs {
                    value = content;
                }
                Ok(Some(value))
            }
            Err(_) => Ok(None),
        }
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        let mut tx = self
            .persy
            .begin()
            .map_err(|e| parse_error(e.persy_error()))?;
        tx.insert(path, value)
            .map_err(|e| parse_error(e.persy_error()))?;
        let prepared = tx.prepare().map_err(|e| parse_error(e.persy_error()))?;
        prepared
            .commit()
            .map_err(|e| parse_error(e.persy_error()))?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        let mut tx = self
            .persy
            .begin()
            .map_err(|e| parse_error(e.persy_error()))?;
        for (id, _content) in self
            .persy
            .scan(path)
            .map_err(|e| parse_error(e.persy_error()))?
        {
            tx.delete(path, &id)
                .map_err(|e| parse_error(e.persy_error()))?;
        }
        let prepared = tx.prepare().map_err(|e| parse_error(e.persy_error()))?;
        prepared
            .commit()
            .map_err(|e| parse_error(e.persy_error()))?;

        Ok(())
    }
}

fn parse_error(err: persy::PersyError) -> Error {
    let kind = match err {
        persy::PersyError::RecordNotFound(_) => ErrorKind::NotFound,
        _ => ErrorKind::Unexpected,
    };

    Error::new(kind, "error from persy").set_source(err)
}
