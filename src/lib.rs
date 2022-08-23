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

//! OpenDAL is the Open Data Access Layer that connect the whole world together.
//!
//! # Supported Services
//!
//! | Services | Description |
//! | -------- | ----------- |
//! | [azblob][crate::services::azblob] | Azure Storage Blob services. |
//! | [fs][crate::services::fs] | POSIX alike file system. |
//! | [gcs][crate::services::gcs] | Google Cloud Storage service. |
//! | [hdfs][crate::services::hdfs] | Hadoop Distributed File System(HDFS). |
//! | [http][crate::services::http] | HTTP read-only backend. |
//! | [memory][crate::services::memory] | In memory backend support. |
//! | [s3][crate::services::s3] | AWS S3 alike services. |
//!
//! # Optional features
//!
//! ## Layers
//!
//! - `layers-retry`: Enable operator retry support.
//! - `layers-metrics`: Enable operator metrics support.
//! - `layers-tracing`: Enable operator tracing support.
//!
//! ## Services
//!
//! - `services-hdfs`: Enable hdfs service support.
//! - `services-http`: Enable http service support.
//!
//! ## Dependencies features
//!
//! - `compress`: Enable object decompress read support.
//! - `rustls`: Use rustls instead openssl for https connection
//! - `serde`: Implement serde::{Serialize,Deserialize} for ObjectMetadata.
//!
//! # Example
//!
//! ```no_run
//! use anyhow::Result;
//! use futures::StreamExt;
//! use futures::TryStreamExt;
//! use opendal::DirEntry;
//! use opendal::DirStreamer;
//! use opendal::Object;
//! use opendal::ObjectMetadata;
//! use opendal::ObjectMode;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Init Operator
//!     let op = Operator::from_env(Scheme::Fs)?;
//!
//!     // Create object handler.
//!     let o = op.object("test_file");
//!
//!     // Write data info object;
//!     o.write("Hello, World!").await?;
//!
//!     // Read data from object;
//!     let bs = o.read().await?;
//!
//!     // Read range from object;
//!     let bs = o.range_read(1..=11).await?;
//!
//!     // Get object's path
//!     let name = o.name();
//!     let path = o.path();
//!
//!     // Fetch more meta about object.
//!     let meta = o.metadata().await?;
//!     let mode = meta.mode();
//!     let length = meta.content_length();
//!     let content_md5 = meta.content_md5();
//!     let etag = meta.etag();
//!
//!     // Delete object.
//!     o.delete().await?;
//!
//!     // List dir object.
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
mod accessor;
pub use accessor::Accessor;
pub use accessor::AccessorMetadata;

mod io;
pub use io::BytesRead;
pub use io::BytesReader;
pub use io::BytesSink;
pub use io::BytesStream;
pub use io::BytesWrite;
pub use io::BytesWriter;

mod operator;
pub use operator::BatchOperator;
pub use operator::Operator;

mod object;
pub use object::DirEntry;
pub use object::DirStream;
pub use object::DirStreamer;
pub use object::Object;
pub use object::ObjectMetadata;
pub use object::ObjectMode;

mod scheme;
pub use scheme::Scheme;

// Public modules, they will be accessed via `opendal::io_util::Xxxx`
pub mod io_util;
pub mod layers;
pub use layers::Layer;
pub mod ops;
pub mod services;

// Private modules, internal use only.
//
// Please don't export any type from this module.
mod error;
mod http_util;
mod path;
