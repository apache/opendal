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
//! | [azblob][services::azblob] | Azure Storage Blob services. |
//! | [fs][services::fs] | POSIX alike file system. |
//! | [ftp][services::ftp] | FTP and FTPS support. |
//! | [gcs][services::gcs] | Google Cloud Storage service. |
//! | [hdfs][services::hdfs] | Hadoop Distributed File System(HDFS). |
//! | [http][services::http] | HTTP read-only backend. |
//! | [ipfs][services::ipfs] | IPFS HTTP Gateway support. |
//! | [ipmfs][services::ipmfs] | IPFS Mutable File System support. |
//! | [memory][services::memory] | In memory backend support. |
//! | [moka][services::moka] | [moka](https://github.com/moka-rs/moka) backend support. |
//! | [obs][services::obs] | Huawei Cloud OBS service. |
//! | [oss][services::oss] | Aliyun Object Storage Service (OSS).|
//! | [redis][services::redis] | Redis service. |
//! | [rocksdb][services::rocksdb] | RocksDB service. |
//! | [s3][services::s3] | AWS S3 alike services. |
//!
//! More services support is tracked at [opendal#5](https://github.com/datafuselabs/opendal/issues/5)
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
//! | [ContentCacheLayer][layers::ContentCacheLayer] | Content cache. |
//! | [ImmutableIndexLayer][layers::ImmutableIndexLayer] | Immutable in-memory index. |
//! | [LoggingLayer][layers::LoggingLayer] | Logging for every operations. |
//! | [MetadataCacheLayer][layers::MetadataCacheLayer] | Metadata cache. |
//! | [MetricsLayer][layers::MetricsLayer] | Metrics for every operations. |
//! | [RetryLayer][layers::RetryLayer] | Retry for failed operations. |
//! | [SubdirLayer][layers::SubdirLayer] | Allow switching directory. |
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
//! use opendal::Object;
//! use opendal::ObjectEntry;
//! use opendal::ObjectMetadata;
//! use opendal::ObjectMode;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Init a fs operator
//!     let op = Operator::from_env(Scheme::Fs)?
//!         // Init with logging layer enabled.
//!         .layer(LoggingLayer)
//!         // Init with retry layer enabled.
//!         .layer(RetryLayer::new(ExponentialBackoff::default()));
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
//!         let mode = entry.mode();
//!     }
//!
//!     Ok(())
//! }
//! ```

// Make sure all our public APIs have docs.
#![warn(missing_docs)]
// Deny unused qualifications.
#![deny(unused_qualifications)]
// Add options below to allow/deny Clippy lints.

// Private module with public types, they will be accessed via `opendal::Xxxx`
mod operator;
pub use operator::BatchOperator;
pub use operator::Operator;

mod object;
pub use object::Object;
pub use object::ObjectEntry;
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

// Public modules, they will be accessed via `opendal::io_util::Xxxx`
pub mod http_util;
pub mod io_util;
pub mod layers;
pub use layers::Layer;
pub mod adapters;
pub mod ops;
pub mod raw;
pub mod services;
pub mod wrappers;

// Private modules, internal use only.
//
// Please don't export any type from this module.
mod path;

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
        assert_eq!(80, size_of::<AccessorMetadata>());
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
