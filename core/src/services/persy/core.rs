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

use crate::*;

#[derive(Clone)]
pub struct PersyCore {
    pub datafile: String,
    pub segment: String,
    pub index: String,
    pub persy: persy::Persy,
}

impl Debug for PersyCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Adapter");
        ds.field("path", &self.datafile);
        ds.field("segment", &self.segment);
        ds.field("index", &self.index);
        ds.finish()
    }
}

impl PersyCore {
    pub fn get(&self, path: &str) -> Result<Option<Buffer>> {
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

    pub fn set(&self, path: &str, value: Buffer) -> Result<()> {
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

    pub fn delete(&self, path: &str) -> Result<()> {
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
