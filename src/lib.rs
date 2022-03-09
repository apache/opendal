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
//!     let w = o.writer();
//!     let n = w
//!        .write_bytes("Hello, World!".to_string().into_bytes())
//!         .await?;
//!
//!     // Read data from file;
//!     let mut r = o.reader();
//!     let mut buf = vec![];
//!     let n = r.read_to_end(&mut buf).await?;
//!
//!     // Read range from file;
//!     let mut r = o.range_reader(10, 1);
//!     let mut buf = vec![];
//!     let n = r.read_to_end(&mut buf).await?;
//!
//!     // Get file's Metadata
//!    let meta = o.metadata().await?;
//!
//!     // List current dir.
//!     let mut obs = op.objects("").map(|o| o.expect("list object"));
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
mod accessor;
pub use accessor::Accessor;

mod io;
pub use io::BoxedAsyncReader;
pub use io::Reader;
pub use io::Writer;

mod layer;
pub use layer::Layer;

mod operator;
pub use operator::Operator;

mod object;
pub use object::Metadata;
pub use object::Object;
pub use object::ObjectMode;
pub use object::ObjectStream;

mod scheme;
pub use scheme::Scheme;

pub mod credential;
pub mod error;
pub mod readers;

pub mod ops;
pub mod services;

#[cfg(test)]
pub mod tests;
