use std::sync::Arc;

use crate::Result;

use super::error::*;

#[derive(Debug)]
pub struct RedbCore {
    #[allow(dead_code)]
    pub(super) datadir: Option<String>,
    pub(super) table: String,
    pub(super) root: String,
    pub(super) db: Arc<redb::Database>,
}

impl RedbCore {
    /// Check if a table exists, otherwise create it.
    pub fn create_table(&self) -> Result<()> {
        // Only one `WriteTransaction` is permitted at same time,
        // applying new one will block until it available.
        //
        // So we first try checking table existence via `ReadTransaction`.
        {
            let read_txn = self.db.begin_read().map_err(parse_transaction_error)?;

            let table_define: redb::TableDefinition<&str, &[u8]> =
                redb::TableDefinition::new(&self.table);

            match read_txn.open_table(table_define) {
                Ok(_) => return Ok(()),
                Err(redb::TableError::TableDoesNotExist(_)) => (),
                Err(e) => return Err(parse_table_error(e)),
            }
        }

        {
            let write_txn = self.db.begin_write().map_err(parse_transaction_error)?;

            let table_define: redb::TableDefinition<&str, &[u8]> =
                redb::TableDefinition::new(&self.table);

            write_txn
                .open_table(table_define)
                .map_err(parse_table_error)?;
            write_txn.commit().map_err(parse_commit_error)?;
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let read_txn = self.db.begin_read().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> =
            redb::TableDefinition::new(&self.table);

        let table = read_txn
            .open_table(table_define)
            .map_err(parse_table_error)?;

        let result = match table.get(key) {
            Ok(Some(v)) => Ok(Some(v.value().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(parse_storage_error(e)),
        }?;
        Ok(result)
    }

    pub fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let write_txn = self.db.begin_write().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> =
            redb::TableDefinition::new(&self.table);

        {
            let mut table = write_txn
                .open_table(table_define)
                .map_err(parse_table_error)?;

            table.insert(key, value).map_err(parse_storage_error)?;
        }

        write_txn.commit().map_err(parse_commit_error)?;
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        let write_txn = self.db.begin_write().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> =
            redb::TableDefinition::new(&self.table);

        {
            let mut table = write_txn
                .open_table(table_define)
                .map_err(parse_table_error)?;

            table.remove(key).map_err(parse_storage_error)?;
        }

        write_txn.commit().map_err(parse_commit_error)?;
        Ok(())
    }

    pub fn iter(&self) -> Result<redb::Range<'static, &'static str, &'static [u8]>> {
        let read_txn = self.db.begin_read().map_err(parse_transaction_error)?;

        let table_define: redb::TableDefinition<&str, &[u8]> =
            redb::TableDefinition::new(&self.table);

        let table = read_txn
            .open_table(table_define)
            .map_err(parse_table_error)?;

        table.range::<&str>(..).map_err(parse_storage_error)
    }
}
