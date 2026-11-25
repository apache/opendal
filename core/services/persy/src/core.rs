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
use std::sync::Mutex;
use std::sync::MutexGuard;

use opendal_core::raw::ServiceOperation;
use opendal_core::*;

/// Context needed to classify an error from this service.
#[derive(Clone, Copy, Debug)]
struct ErrorContext {
    service_operation: ServiceOperation,
}

impl ErrorContext {
    const fn new(service_operation: ServiceOperation) -> Self {
        Self { service_operation }
    }
}

#[derive(Clone)]
pub struct PersyCore {
    pub datafile: String,
    pub segment: String,
    pub index: String,
    pub persy: persy::Persy,
    /// Serializes write transactions so that reading the index and updating the record
    /// happen without another writer committing in between. `persy` acquires its data
    /// locks only in `prepare`, so concurrent transactions would otherwise act on stale
    /// index entries and the loser would fail with `RecordNotFound` instead of observing
    /// the committed write. `persy` holds an exclusive file lock for the datafile, so this
    /// lock covers every writer of the database.
    pub write_lock: Arc<Mutex<()>>,
}

impl Debug for PersyCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersyCore")
            .field("path", &self.datafile)
            .field("segment", &self.segment)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl PersyCore {
    fn lock_write(&self) -> MutexGuard<'_, ()> {
        // A panic while writing must not disable writes for the rest of the database.
        self.write_lock
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }

    /// Create the configured segment and index if they are missing.
    ///
    /// `persy` initializes a datafile only while creating the file itself, so a datafile
    /// that already exists must be initialized explicitly before it can be used.
    pub fn init(&self) -> Result<()> {
        let mut tx = self.persy.begin().map_err(|err| {
            parse_error(ErrorContext::new(ServiceOperation("BeginTransaction")), err)
        })?;

        if !tx
            .exists_segment(&self.segment)
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("ExistsSegment")), err))?
        {
            tx.create_segment(&self.segment).map_err(|err| {
                parse_error(ErrorContext::new(ServiceOperation("CreateSegment")), err)
            })?;
        }

        if !tx
            .exists_index(&self.index)
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("ExistsIndex")), err))?
        {
            tx.create_index::<String, persy::PersyId>(&self.index, persy::ValueMode::Replace)
                .map_err(|err| {
                    parse_error(ErrorContext::new(ServiceOperation("CreateIndex")), err)
                })?;
        }

        let prepared = tx.prepare().map_err(|err| {
            parse_error(
                ErrorContext::new(ServiceOperation("PrepareTransaction")),
                err,
            )
        })?;
        prepared.commit().map_err(|err| {
            parse_error(
                ErrorContext::new(ServiceOperation("CommitTransaction")),
                err,
            )
        })?;

        Ok(())
    }

    pub fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let mut read_id = self
            .persy
            .get::<String, persy::PersyId>(&self.index, &path.to_string())
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("GetIndex")), err))?;
        if let Some(id) = read_id.next() {
            let value = self.persy.read(&self.segment, &id).map_err(|err| {
                parse_error(ErrorContext::new(ServiceOperation("ReadRecord")), err)
            })?;
            return Ok(value.map(Buffer::from));
        }

        Ok(None)
    }

    pub fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let _guard = self.lock_write();

        let mut tx = self.persy.begin().map_err(|err| {
            parse_error(ErrorContext::new(ServiceOperation("BeginTransaction")), err)
        })?;

        let path = path.to_string();
        let value = value.to_vec();

        // Read the record id in this transaction so that overwriting a path updates the
        // record the index already refers to instead of inserting an orphaned one.
        let id = tx
            .one::<String, persy::PersyId>(&self.index, &path)
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("GetIndex")), err))?;

        match id {
            Some(id) => tx.update(&self.segment, &id, &value).map_err(|err| {
                parse_error(ErrorContext::new(ServiceOperation("UpdateRecord")), err)
            })?,
            None => {
                let id = tx.insert(&self.segment, &value).map_err(|err| {
                    parse_error(ErrorContext::new(ServiceOperation("InsertRecord")), err)
                })?;
                tx.put::<String, persy::PersyId>(&self.index, path, id)
                    .map_err(|err| {
                        parse_error(ErrorContext::new(ServiceOperation("PutIndex")), err)
                    })?;
            }
        }

        let prepared = tx.prepare().map_err(|err| {
            parse_error(
                ErrorContext::new(ServiceOperation("PrepareTransaction")),
                err,
            )
        })?;
        prepared.commit().map_err(|err| {
            parse_error(
                ErrorContext::new(ServiceOperation("CommitTransaction")),
                err,
            )
        })?;

        Ok(())
    }

    pub fn delete(&self, path: &str) -> Result<()> {
        let _guard = self.lock_write();

        let mut tx = self.persy.begin().map_err(|err| {
            parse_error(ErrorContext::new(ServiceOperation("BeginTransaction")), err)
        })?;

        let path = path.to_string();

        // Look up the record id inside the transaction that removes the record and its
        // index entry, so both always refer to the same record.
        let id = tx
            .one::<String, persy::PersyId>(&self.index, &path)
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("GetIndex")), err))?;
        let Some(id) = id else {
            return Ok(());
        };

        tx.delete(&self.segment, &id)
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("DeleteRecord")), err))?;
        tx.remove::<String, persy::PersyId>(&self.index, path, Some(id))
            .map_err(|err| parse_error(ErrorContext::new(ServiceOperation("RemoveIndex")), err))?;

        let prepared = tx.prepare().map_err(|err| {
            parse_error(
                ErrorContext::new(ServiceOperation("PrepareTransaction")),
                err,
            )
        })?;
        prepared.commit().map_err(|err| {
            parse_error(
                ErrorContext::new(ServiceOperation("CommitTransaction")),
                err,
            )
        })?;

        Ok(())
    }
}

fn parse_error<T: Into<persy::PersyError>>(ctx: ErrorContext, err: persy::PE<T>) -> Error {
    let err = err.persy_error();
    let kind = match err {
        persy::PersyError::RecordNotFound(_) => ErrorKind::NotFound,
        _ => ErrorKind::Unexpected,
    };

    Error::new(kind, "error from persy")
        .with_context("service_operation", ctx.service_operation.0)
        .set_source(err)
}
