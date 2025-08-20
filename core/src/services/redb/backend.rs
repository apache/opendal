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
use std::sync::Arc;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::RedbConfig;
use crate::Builder;
use crate::Error;
use crate::ErrorKind;
use crate::Scheme;
use crate::*;

impl Configurator for RedbConfig {
    type Builder = RedbBuilder;
    fn into_builder(self) -> Self::Builder {
        RedbBuilder {
            config: self,
            database: None,
        }
    }
}

/// Redb service support.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct RedbBuilder {
    config: RedbConfig,

    database: Option<Arc<redb::Database>>,
}

impl RedbBuilder {
    /// Set the database for Redb.
    ///
    /// This method should be called when you want to
    /// use multiple tables of one database because
    /// Redb doesn't allow opening a database that have been opened.
    ///
    /// <div class="warning">
    ///
    /// `datadir` and `database` should not be set simultaneously.
    /// If both are set, `database` will take precedence.
    ///
    /// </div>
    pub fn database(mut self, db: Arc<redb::Database>) -> Self {
        self.database = Some(db);
        self
    }

    /// Set the path to the redb data directory. Will create if not exists.
    ///
    ///
    /// <div class="warning">
    ///
    /// Opening redb database via `datadir` takes away the ability to access multiple redb tables.
    /// If you need to access multiple redb tables, the correct solution is to
    /// create an `Arc<redb::database>` beforehand and then share it via [`database`]
    /// with multiple builders where every builder will open one redb table.
    ///
    /// </div>
    ///
    /// [`database`]: RedbBuilder::database
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }

    /// Set the table name for Redb. Will create if not exists.
    pub fn table(mut self, table: &str) -> Self {
        self.config.table = Some(table.into());
        self
    }

    /// Set the root for Redb.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = Some(path.into());
        self
    }
}

impl Builder for RedbBuilder {
    type Config = RedbConfig;

    fn build(self) -> Result<impl Access> {
        let table_name = self.config.table.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "table is required but not set")
                .with_context("service", Scheme::Redb)
        })?;

        let (datadir, db) = if let Some(db) = self.database {
            (None, db)
        } else {
            let datadir = self.config.datadir.ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                    .with_context("service", Scheme::Redb)
            })?;

            let db = redb::Database::create(&datadir)
                .map_err(parse_database_error)?
                .into();

            (Some(datadir), db)
        };

        create_table(&db, &table_name)?;

        Ok(RedbBackend::new(Adapter {
            datadir,
            table: table_name,
            db,
        })
        .with_root(self.config.root.as_deref().unwrap_or_default()))
    }
}

/// Backend for Redb services.
pub type RedbBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    datadir: Option<String>,
    table: String,
    db: Arc<redb::Database>,
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
            Scheme::Redb,
            &self.table,
            Capability {
                read: true,
                write: true,
                shared: false,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let read_txn = self.db.begin_read().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> =
            redb::TableDefinition::new(&self.table);

        let table = read_txn
            .open_table(table_define)
            .map_err(parse_table_error)?;

        let result = match table.get(path) {
            Ok(Some(v)) => Ok(Some(v.value().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(parse_storage_error(e)),
        }?;
        Ok(result.map(Buffer::from))
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        let write_txn = self.db.begin_write().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> =
            redb::TableDefinition::new(&self.table);

        {
            let mut table = write_txn
                .open_table(table_define)
                .map_err(parse_table_error)?;

            table
                .insert(path, &*value.to_vec())
                .map_err(parse_storage_error)?;
        }

        write_txn.commit().map_err(parse_commit_error)?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let write_txn = self.db.begin_write().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> =
            redb::TableDefinition::new(&self.table);

        {
            let mut table = write_txn
                .open_table(table_define)
                .map_err(parse_table_error)?;

            table.remove(path).map_err(parse_storage_error)?;
        }

        write_txn.commit().map_err(parse_commit_error)?;
        Ok(())
    }
}

fn parse_transaction_error(e: redb::TransactionError) -> Error {
    Error::new(ErrorKind::Unexpected, "error from redb").set_source(e)
}

fn parse_table_error(e: redb::TableError) -> Error {
    match e {
        redb::TableError::TableDoesNotExist(_) => {
            Error::new(ErrorKind::NotFound, "error from redb").set_source(e)
        }
        _ => Error::new(ErrorKind::Unexpected, "error from redb").set_source(e),
    }
}

fn parse_storage_error(e: redb::StorageError) -> Error {
    Error::new(ErrorKind::Unexpected, "error from redb").set_source(e)
}

fn parse_database_error(e: redb::DatabaseError) -> Error {
    Error::new(ErrorKind::Unexpected, "error from redb").set_source(e)
}

fn parse_commit_error(e: redb::CommitError) -> Error {
    Error::new(ErrorKind::Unexpected, "error from redb").set_source(e)
}

/// Check if a table exists, otherwise create it.
fn create_table(db: &redb::Database, table: &str) -> Result<()> {
    // Only one `WriteTransaction` is permitted at same time,
    // applying new one will block until it available.
    //
    // So we first try checking table existence via `ReadTransaction`.
    {
        let read_txn = db.begin_read().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(table);

        match read_txn.open_table(table_define) {
            Ok(_) => return Ok(()),
            Err(redb::TableError::TableDoesNotExist(_)) => (),
            Err(e) => return Err(parse_table_error(e)),
        }
    }

    {
        let write_txn = db.begin_write().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(table);

        write_txn
            .open_table(table_define)
            .map_err(parse_table_error)?;
        write_txn.commit().map_err(parse_commit_error)?;
    }

    Ok(())
}
