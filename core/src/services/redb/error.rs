use crate::{Error, ErrorKind};

pub fn parse_transaction_error(e: redb::TransactionError) -> Error {
    Error::new(ErrorKind::Unexpected, "error from redb").set_source(e)
}

pub fn parse_table_error(e: redb::TableError) -> Error {
    match e {
        redb::TableError::TableDoesNotExist(_) => {
            Error::new(ErrorKind::NotFound, "error from redb").set_source(e)
        }
        _ => Error::new(ErrorKind::Unexpected, "error from redb").set_source(e),
    }
}

pub fn parse_storage_error(e: redb::StorageError) -> Error {
    Error::new(ErrorKind::Unexpected, "error from redb").set_source(e)
}

pub fn parse_database_error(e: redb::DatabaseError) -> Error {
    Error::new(ErrorKind::Unexpected, "error from redb").set_source(e)
}

pub fn parse_commit_error(e: redb::CommitError) -> Error {
    Error::new(ErrorKind::Unexpected, "error from redb").set_source(e)
}
