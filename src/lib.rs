// Copyright 2022 Datafuse Labs
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
//! - Documentation: All docs are carried byself, visit [`docs`] for more.
//! - Services: All supported services could be found at [`services`].
//! - Layers: All builtin layer could be found at [`layers`].
//! - Features: All features could be found at [`features`][docs::features].
//!
//! # Quick Start
//!
//! ```no_run
//! use opendal::layers::LoggingLayer;
//! use opendal::services;
//! use opendal::Operator;
//! use opendal::Result;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Pick a builder and configure it.
//!     let mut builder = services::S3::default();
//!     builder.bucket("test");
//!
//!     // Init an operator
//!     let op = Operator::create(builder)?
//!         // Init with logging layer enabled.
//!         .layer(LoggingLayer::default())
//!         .finish();
//!
//!     // Create object handler.
//!     let mut o = op.object("test_file");
//!
//!     // Write data
//!     o.write("Hello, World!").await?;
//!
//!     // Read data
//!     let bs = o.read().await?;
//!
//!     // Fetch metadata
//!     let meta = o.stat().await?;
//!     let mode = meta.mode();
//!     let length = meta.content_length();
//!
//!     // Delete
//!     o.delete().await?;
//!
//!     Ok(())
//! }
//! ```

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
pub use object::ObjectMetakey;
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

// Public modules, they will be accessed like `opendal::layers::Xxxx`
#[cfg(feature = "docs")]
pub mod docs;
pub mod layers;
pub mod ops;
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
        assert_eq!(112, size_of::<BatchOperator>());
        assert_eq!(32, size_of::<Object>());
        assert_eq!(192, size_of::<ObjectMetadata>());
        assert_eq!(1, size_of::<ObjectMode>());
        assert_eq!(64, size_of::<ObjectMultipart>());
        assert_eq!(32, size_of::<ObjectPart>());
        assert_eq!(24, size_of::<Scheme>());
    }
}
