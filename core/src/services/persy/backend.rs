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

use persy;
use tokio::task;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::PersyConfig;
use crate::Builder;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;
use crate::*;

impl Configurator for PersyConfig {
    type Builder = PersyBuilder;
    fn into_builder(self) -> Self::Builder {
        PersyBuilder { config: self }
    }
}

/// persy service support.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct PersyBuilder {
    config: PersyConfig,
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
    const SCHEME: Scheme = Scheme::Persy;
    type Config = PersyConfig;

    fn build(self) -> Result<impl Access> {
        let datafile_path = self.config.datafile.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datafile is required but not set")
                .with_context("service", Scheme::Persy)
        })?;

        let segment_name = self.config.segment.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "segment is required but not set")
                .with_context("service", Scheme::Persy)
        })?;

        let segment = segment_name.clone();

        let index_name = self.config.index.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "index is required but not set")
                .with_context("service", Scheme::Persy)
        })?;

        let index = index_name.clone();

        let persy = persy::OpenOptions::new()
            .create(true)
            .prepare_with(move |p| init(p, &segment_name, &index_name))
            .open(&datafile_path)
            .map_err(|e| {
                Error::new(ErrorKind::ConfigInvalid, "open db")
                    .with_context("service", Scheme::Persy)
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

        Ok(PersyBackend::new(Adapter {
            datafile: datafile_path,
            segment,
            index,
            persy,
        }))
    }
}

/// Backend for persy services.
pub type PersyBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    datafile: String,
    segment: String,
    index: String,
    persy: persy::Persy,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.datafile);
        ds.field("segment", &self.segment);
        ds.field("index", &self.index);
        ds.finish()
    }
}

impl kv::Adapter for Adapter {
    type Scanner = ();

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::Persy,
            &self.datafile,
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
        let cloned_self = self.clone();
        let cloned_path = path.to_string();
        task::spawn_blocking(move || cloned_self.blocking_get(cloned_path.as_str()))
            .await
            .map_err(new_task_join_error)
            .and_then(|inner_result| inner_result)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Buffer>> {
        let mut read_id = self
            .persy
            .get::<String, persy::PersyId>(&self.index, &path.to_string())
            .map_err(parse_error)?;
        if let Some(id) = read_id.next() {
            let value = self.persy.read(&self.segment, &id).map_err(parse_error)?;
            return Ok(value.map(Buffer::from));
        }

        Ok(None)
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let cloned_path = path.to_string();
        let cloned_self = self.clone();

        task::spawn_blocking(move || cloned_self.blocking_set(cloned_path.as_str(), value))
            .await
            .map_err(new_task_join_error)
            .and_then(|inner_result| inner_result)
    }

    fn blocking_set(&self, path: &str, value: Buffer) -> Result<()> {
        let mut tx = self.persy.begin().map_err(parse_error)?;
        let id = tx
            .insert(&self.segment, &value.to_vec())
            .map_err(parse_error)?;

        tx.put::<String, persy::PersyId>(&self.index, path.to_string(), id)
            .map_err(parse_error)?;
        let prepared = tx.prepare().map_err(parse_error)?;
        prepared.commit().map_err(parse_error)?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let cloned_path = path.to_string();
        let cloned_self = self.clone();

        task::spawn_blocking(move || cloned_self.blocking_delete(cloned_path.as_str()))
            .await
            .map_err(new_task_join_error)
            .and_then(|inner_result| inner_result)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        let mut delete_id = self
            .persy
            .get::<String, persy::PersyId>(&self.index, &path.to_string())
            .map_err(parse_error)?;
        if let Some(id) = delete_id.next() {
            // Begin a transaction.
            let mut tx = self.persy.begin().map_err(parse_error)?;
            // Delete the record.
            tx.delete(&self.segment, &id).map_err(parse_error)?;
            // Remove the index.
            tx.remove::<String, persy::PersyId>(&self.index, path.to_string(), Some(id))
                .map_err(parse_error)?;
            // Commit the tx.
            let prepared = tx.prepare().map_err(parse_error)?;
            prepared.commit().map_err(parse_error)?;
        }

        Ok(())
    }
}

fn parse_error<T: Into<persy::PersyError>>(err: persy::PE<T>) -> Error {
    let err: persy::PersyError = err.persy_error();
    let kind = match err {
        persy::PersyError::RecordNotFound(_) => ErrorKind::NotFound,
        _ => ErrorKind::Unexpected,
    };

    Error::new(kind, "error from persy").set_source(err)
}
