use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use async_trait::async_trait;
use rusqlite::{params, Connection};

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

#[derive(Default)]
pub struct SqliteBuilder {
    connection_string: Option<String>,

    table: Option<String>,
    key_field: Option<String>,
    value_field: Option<String>,
    root: Option<String>,
}

impl Debug for SqliteBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("SqliteBuilder");
        ds.field("connection_string", &self.connection_string);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.field("root", &self.root);
        ds.finish()
    }
}

impl SqliteBuilder {
    pub fn connection_string(&mut self, v: &str) -> &mut Self {
        if !v.is_empty() {
            self.connection_string = Some(v.to_string());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }

    /// Set the table name of the sqlite service to read/write.
    pub fn table(&mut self, table: &str) -> &mut Self {
        if !table.is_empty() {
            self.table = Some(table.to_string());
        }
        self
    }

    /// Set the key field name of the sqlite service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the sqlite service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.value_field = Some(value_field.to_string());
        }
        self
    }
}

impl Builder for SqliteBuilder {
    const SCHEME: Scheme = Scheme::Sqlite;
    type Accessor = SqliteBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = SqliteBuilder::default();
        map.get("connection_string")
            .map(|v| builder.connection_string(v));
        map.get("table").map(|v| builder.table(v));
        map.get("key_field").map(|v| builder.key_field(v));
        map.get("value_field").map(|v| builder.value_field(v));
        map.get("root").map(|v| builder.root(v));
        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let connection_string = match self.connection_string.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "connection_string is required but not set",
                )
                .with_context("service", Scheme::Sqlite))
            }
        };
        let table = match self.table.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "table is empty")
                    .with_context("service", Scheme::Postgresql))
            }
        };
        let key_field = match self.key_field.clone() {
            Some(v) => v,
            None => "key".to_string(),
        };
        let value_field = match self.value_field.clone() {
            Some(v) => v,
            None => "value".to_string(),
        };
        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );
        Ok(SqliteBackend::new(Adapter {
            connection_string: connection_string,
            table: table,
            key_field: key_field,
            value_field: value_field,
        })
        .with_root(&root))
    }
}

pub type SqliteBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    connection_string: String,

    table: String,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("SqliteAdapter");
        ds.field("connection_string", &self.connection_string);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.finish()
    }
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Sqlite,
            &self.table,
            Capability {
                read: true,
                write: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let query = format!(
            "SELECT {} FROM {} WHERE `{}` = $1 LIMIT 1",
            self.value_field, self.table, self.key_field
        );
        let conn = Connection::open(self.connection_string.clone()).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite open error").set_source(err)
        })?;
        let mut statement = conn.prepare(&query).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite prepared query error").set_source(err)
        })?;
        let result = statement
            .query_row(&[path], |row| Ok({ row.get(0)? }))
            .map_err(|err| Error::new(ErrorKind::Unexpected, "Sqlite query error").set_source(err));
        return result;
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        let query = format!(
            "INSERT OR REPLACE INTO `{}` (`{}`, `{}`) VALUES ($1, $2)",
            self.table, self.key_field, self.value_field
        );
        let conn = Connection::open(self.connection_string.clone()).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite open error").set_source(err)
        })?;
        let mut statement = conn.prepare(&query).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite prepared query error").set_source(err)
        })?;
        statement.execute(params![path, value]).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite qupdate error").set_source(err)
        })?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let conn = Connection::open(self.connection_string.clone()).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite open error").set_source(err)
        })?;
        let query = format!("DELETE FROM {} WHERE `{}` = $1", self.table, self.key_field);
        let mut statement = conn.prepare(&query).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite prepared error").set_source(err)
        })?;
        statement.execute(&[path]).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "Sqlite delete error").set_source(err)
        })?;
        Ok(())
    }
}
