// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![cfg_attr(docs, feature(doc_auto_cfg))]

//! Apache OpenDAL™ is the Open Data Access Layer to **freely** access data.
//!
//! - Documentation: All docs are carried by self, visit [`docs`] for more.
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
//!     let op = Operator::new(builder)?
//!         // Init with logging layer enabled.
//!         .layer(LoggingLayer::default())
//!         .finish();
//!
//!     // Write data
//!     op.write("hello.txt", "Hello, World!").await?;
//!
//!     // Read data
//!     let bs = op.read("hello.txt").await?;
//!
//!     // Fetch metadata
//!     let meta = op.stat("hello.txt").await?;
//!     let mode = meta.mode();
//!     let length = meta.content_length();
//!
//!     // Delete
//!     op.delete("hello.txt").await?;
//!
//!     Ok(())
//! }
//! ```

// Make sure all our public APIs have docs.
#![warn(missing_docs)]
// Deny unused qualifications.
#![deny(unused_qualifications)]

// Private module with public types, they will be accessed via `opendal::Xxxx`
mod types;
pub use types::*;

// Public modules, they will be accessed like `opendal::layers::Xxxx`
#[cfg(docs)]
pub mod docs;
pub mod layers;
pub mod raw;
pub mod services;

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    /// This is not a real test case.
    ///
    /// We assert our public structs here to make sure we don't introduce
    /// unexpected struct/enum size change.
    #[test]
    fn assert_size() {
        assert_eq!(24, size_of::<Operator>());
        assert_eq!(264, size_of::<Entry>());
        assert_eq!(240, size_of::<Metadata>());
        assert_eq!(1, size_of::<EntryMode>());
        assert_eq!(24, size_of::<Scheme>());
    }

    /// This is used to make sure our public API implement Send + Sync
    trait AssertSendSync: Send + Sync {}
    impl AssertSendSync for Entry {}
    impl AssertSendSync for Capability {}
    impl AssertSendSync for Error {}
    impl AssertSendSync for Reader {}
    impl AssertSendSync for Writer {}
    impl AssertSendSync for Lister {}
    impl AssertSendSync for Operator {}
    impl AssertSendSync for BlockingReader {}
    impl AssertSendSync for BlockingWriter {}
    impl AssertSendSync for BlockingLister {}
    impl AssertSendSync for BlockingOperator {}
}
