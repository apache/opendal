use std::sync::Arc;

use crate::Result;

use super::error::*;

#[derive(Debug)]
pub struct RedbCore {
    pub(super) datadir: Option<String>,
    pub(super) table: String,
    pub(super) root: String,
    pub(super) db: Arc<redb::Database>,
}

impl RedbCore {
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
}
