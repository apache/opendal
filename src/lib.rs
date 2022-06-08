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
//! | [hdfs][crate::services::hdfs] | Hadoop Distributed File System(HDFS). |
//! | [memory][crate::services::memory] | In memory backend support. |
//! | [s3][crate::services::s3] | AWS S3 alike services. |
//!
//! # Optional features
//!
//! - `compress`: Enable object decompress read support.
//! - `retry`: Enable operator retry support.
//! - `services-hdfs`: Enable hdfs service support.
//!
//! # Example
//!
//! ```no_run
//! use anyhow::Result;
//! use futures::StreamExt;
//! use opendal::services::fs;
//! use opendal::ObjectMetadata;
//! use opendal::Object;
//! use opendal::ObjectMode;
//! use opendal::ObjectStreamer;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Init Operator
//!     let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);
//!
//!     // Create object handler.
//!     let o: Object = op.object("test_file");
//!
//!     // Write data info object;
//!     let _: () = o.write("Hello, World!").await?;
//!
//!     // Read data from object;
//!     let bs: Vec<u8> = o.read().await?;
//!
//!     // Read range from object;
//!     let bs: Vec<u8> = o.range_read(1..=11).await?;
//!
//!     // Get object's Metadata
//!     let meta: ObjectMetadata = o.metadata().await?;
//!     let name: &str = meta.name();
//!     let path: &str = meta.path();
//!     let mode: ObjectMode = meta.mode();
//!     let length: u64 = meta.content_length();
//!     let content_md5: Option<String> = meta.content_md5();
//!
//!     // Delete object.
//!     let _: () = o.delete().await?;
//!
//!     // List dir object.
//!     let o: Object = op.object("test_dir/");
//!     let mut obs: ObjectStreamer = o.list().await?;
//!     while let Some(entry) = obs.next().await {
//!         let entry: Object = entry?;
//!     }
//!
//!     Ok(())
//! }
//! ```

// Make sure all our public APIs have docs.
#![warn(missing_docs)]

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

mod layers;
pub use layers::Layer;

mod operator;
pub use operator::Operator;

mod object;
pub use object::DirEntry;
pub use object::DirStream;
pub use object::DirStreamer;
pub use object::Object;
pub use object::ObjectMetadata;
#[deprecated = "Metadata has been deprecated, use ObjectMetadata instead"]
pub use object::ObjectMetadata as Metadata;
pub use object::ObjectMode;
pub use object::ObjectStream;
pub use object::ObjectStreamer;

mod scheme;
pub use scheme::Scheme;

// Public modules, they will be accessed via `opendal::io_util::Xxxx`
pub mod io_util;
pub mod ops;
pub mod services;

// Private modules, internal use only.
//
// Please don't export any type from this module.
mod error;
