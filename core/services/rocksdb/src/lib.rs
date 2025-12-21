/// Default scheme for rocksdb service.
pub const ROCKSDB_SCHEME: &str = "rocksdb";

mod backend;
mod config;
mod core;
mod deleter;
mod lister;
mod writer;

pub use crate::backend::RocksdbBuilder as RocksDB;
pub use crate::config::RocksdbConfig;

#[ctor::ctor]
fn register_rocksdb_service() {
    opendal_core::DEFAULT_OPERATOR_REGISTRY.register::<RocksDB>(ROCKSDB_SCHEME);
}
