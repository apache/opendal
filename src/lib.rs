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
//! # Example
//!
//! ```
//! use anyhow::Result;
//! use futures::AsyncReadExt;
//! use futures::StreamExt;
//! use opendal::services::fs;
//! use opendal::ObjectMode;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);
//!
//!     let o = op.object("test_file");
//!
//!     // Write data info file;
//!     let _ = o.write("Hello, World!").await?;
//!
//!     // Read data from file;
//!     let bs = o.read().await?;
//!
//!     // Read range from file;
//!     let bs = o.range_read(1..=11).await?;
//!
//!     // Get file's Metadata
//!    let meta = o.metadata().await?;
//!
//!     // List dir.
//!     let mut obs = op.objects("").await?.map(|o| o.expect("list object"));
//!     while let Some(o) = obs.next().await {
//!         let meta = o.metadata().await?;
//!         let path = meta.path();
//!         let mode = meta.mode();
//!         let length = meta.content_length();
//!     }
//!
//!     // Delete file.
//!     o.delete().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Supported Services
//!
//! - [fs][crate::services::fs]: POSIX alike file system.
//! - [memory][crate::services::memory]: In memory backend support.
//! - [s3][crate::services::s3]: AWS services like S3.

/// Private module with public types, they will be accessed via `opendal::Xxxx`
mod accessor;
pub use accessor::Accessor;

mod io;
pub use io::BytesRead;
pub use io::BytesReader;
pub use io::BytesSink;
pub use io::BytesStream;
pub use io::BytesWrite;
pub use io::BytesWriter;

mod layer;
pub use layer::Layer;

mod operator;
pub use operator::Operator;

mod object;
pub use object::Metadata;
pub use object::Object;
pub use object::ObjectMode;
pub use object::ObjectStream;
pub use object::ObjectStreamer;

mod scheme;
pub use scheme::Scheme;

/// Public modules, they will be accessed via `opendal::io_util::Xxxx`
pub mod io_util;
pub mod ops;
pub mod services;

/// Private modules, internal use only.
///
/// Please don't export any type from this module.
mod error;

#[deprecated]
pub mod readers;
