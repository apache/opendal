// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! OpenDAL is the Open Data Access Layer to **freely**, **painlessly**, and **efficiently** access data.
//!
//! # Services
//!
//! `Service` represents a backend scheme that OpenDAL supported.
//!
//! OpenDAL supports the following services now:
//!
//! | Services | Description |
//! | -------- | ----------- |
//! | [azblob][services::Azblob] | Azure Storage Blob services. |
//! | [azdfs][services::Azdfs] | Azure Data Lake Storage Gen2 services. |
//! | [fs][services::Fs] | POSIX alike file system. |
//! | [ftp][services::Ftp] | FTP and FTPS support. |
//! | [gcs][services::Gcs] | Google Cloud Storage service. |
//! | [ghac][services::Ghac] | Github Action Cache service. |
//! | [hdfs][services::Hdfs] | Hadoop Distributed File System(HDFS). |
//! | [http][services::Http] | HTTP read-only backend. |
//! | [ipfs][services::Ipfs] | IPFS HTTP Gateway support. |
//! | [ipmfs][services::Ipmfs] | IPFS Mutable File System support. |
//! | [memcached][services::Memcached] | Memcached serivce. |
//! | [memory][services::Memory] | In memory backend support. |
//! | [moka][services::Moka] | [moka](https://github.com/moka-rs/moka) backend support. |
//! | [obs][services::Obs] | Huawei Cloud OBS service. |
//! | [oss][services::Oss] | Aliyun Object Storage Service (OSS).|
//! | [redis][services::Redis] | Redis service. |
//! | [rocksdb][services::Rocksdb] | RocksDB service. |
//! | [s3][services::S3] | AWS S3 alike services. |
//! | [webdav][services::Webdav] | WebDAV services. |
//!
//! - Different services capabilities could be found at [`services`]
//! - More services support is tracked at [opendal#5](https://github.com/datafuselabs/opendal/issues/5)
//!
//! # Layers
//!
//! `Layer` is the mechanism to intercept operations.
//!
//! OpenDAL supports the following layers now:
//!
//! | Layers | Description |
//! | -------- | ----------- |
//! | [ConcurrentLimitLayer][layers::ConcurrentLimitLayer] | Concurrent request limit. |
//! | [ImmutableIndexLayer][layers::ImmutableIndexLayer] | Immutable in-memory index. |
//! | [LoggingLayer][layers::LoggingLayer] | Logging for every operations. |
//! | [MetricsLayer][layers::MetricsLayer] | Metrics for every operations. |
//! | [RetryLayer][layers::RetryLayer] | Retry for failed operations. |
//! | [TracingLayer][layers::TracingLayer] | Tracing for every operations. |
//!
//! # Optional features
//!
//! ## Layers
//!
//! - `layers-all`: Enable all layers support.
//! - `layers-metrics`: Enable operator metrics support.
//! - `layers-tracing`: Enable operator tracing support.
//!
//! ## Services
//!
//! - `services-ftp`: Enable ftp service support.
//! - `services-hdfs`: Enable hdfs service support.
//! - `services-moka`: Enable moka service support.
//! - `services-ipfs`: Enable ipfs service support.
//! - `services-redis`: Enable redis service support.
//! - `services-rocksdb`: Enable rocksdb service support.
//!
//! ## Dependencies features
//!
//! - `compress`: Enable object decompress read support.
//! - `rustls`: Enable TLS functionality provided by `rustls`, enabled by default
//! - `native-tls`: Enable TLS functionality provided by `native-tls`
//! - `native-tls-vendored`: Enable the `vendored` feature of `native-tls`
//!
//! # Examples
//!
//! More examples could be found at <https://opendal.databend.rs/examples/index.html>
//!
//! ```no_run
//! use anyhow::Result;
//! use backon::ExponentialBackoff;
//! use futures::StreamExt;
//! use futures::TryStreamExt;
//! use opendal::layers::LoggingLayer;
//! use opendal::layers::RetryLayer;
//! use opendal::services;
//! use opendal::Object;
//! use opendal::ObjectMetadata;
//! use opendal::ObjectMode;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Init a fs operator
//!     let op = Operator::from_env::<services::Fs>()?
//!         // Init with logging layer enabled.
//!         .layer(LoggingLayer::default())
//!         // Init with retry layer enabled.
//!         .layer(RetryLayer::new(ExponentialBackoff::default()))
//!         .finish();
//!
//!     // Create object handler.
//!     let o = op.object("test_file");
//!
//!     // Write data
//!     o.write("Hello, World!").await?;
//!
//!     // Read data
//!     let bs = o.read().await?;
//!
//!     // Fetch metadata
//!     let name = o.name();
//!     let path = o.path();
//!     let meta = o.metadata().await?;
//!     let mode = meta.mode();
//!     let length = meta.content_length();
//!
//!     // Delete
//!     o.delete().await?;
//!
//!     // Readdir
//!     let o = op.object("test_dir/");
//!     let mut ds = o.list().await?;
//!     while let Some(entry) = ds.try_next().await? {
//!         let path = entry.path();
//!         let mode = entry.mode().await?;
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Documentation
//!
//! OpenDAL carries all it's documentation in crate itself.
//!
//! More docs about OpenDAL could be found at [`docs`].

// Make sure all our public APIs have docs.
#![warn(missing_docs)]
// Deny unused qualifications.
#![deny(unused_qualifications)]

// Private module with public types, they will be accessed via `opendal::Xxxx`
mod builder;
pub use builder::Builder;

mod operator;
pub use operator::BatchOperator;
pub use operator::Operator;
pub use operator::OperatorBuilder;
pub use operator::OperatorMetadata;

mod object;
pub use object::Object;
pub use object::ObjectLister;
pub use object::ObjectMetadata;
pub use object::ObjectMode;
pub use object::ObjectMultipart;
pub use object::ObjectPart;
pub use object::ObjectReader;

mod scheme;
pub use scheme::Scheme;

mod error;
pub use error::Error;
pub use error::ErrorKind;
pub use error::Result;

mod ops;
pub use ops::OpAbortMultipart;
pub use ops::OpCompleteMultipart;
pub use ops::OpCreate;
pub use ops::OpCreateMultipart;
pub use ops::OpDelete;
pub use ops::OpList;
pub use ops::OpPresign;
pub use ops::OpRead;
pub use ops::OpStat;
pub use ops::OpWrite;
pub use ops::OpWriteMultipart;
pub use ops::PresignOperation;

// Public modules, they will be accessed like `opendal::layers::Xxxx`
#[cfg(feature = "docs")]
pub mod docs;
pub mod layers;
pub mod raw;
pub mod services;

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;
    use crate::raw::*;

    /// This is not a real test case.
    ///
    /// We assert our public structs here to make sure we don't introduce
    /// unexpected struct/enum size change.
    #[test]
    fn assert_size() {
        assert_eq!(88, size_of::<AccessorMetadata>());
        assert_eq!(16, size_of::<Operator>());
        assert_eq!(16, size_of::<BatchOperator>());
        assert_eq!(184, size_of::<ObjectEntry>());
        assert_eq!(48, size_of::<Object>());
        assert_eq!(160, size_of::<ObjectMetadata>());
        assert_eq!(1, size_of::<ObjectMode>());
        assert_eq!(64, size_of::<ObjectMultipart>());
        assert_eq!(32, size_of::<ObjectPart>());
        assert_eq!(24, size_of::<Scheme>());
    }
}
