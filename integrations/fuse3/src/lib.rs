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

//! `fuse3_opendal` is an [`fuse3`](https://github.com/Sherlock-Holo/fuse3) implementation using opendal.
//!
//! This crate can help you to access ANY storage services by mounting locally by [`FUSE`](https://www.kernel.org/doc/html/next/filesystems/fuse.html).
//!
//! ```
//! use fuse3::path::Session;
//! use fuse3::MountOptions;
//! use fuse3::Result;
//! use fuse3_opendal::Filesystem;
//! use opendal::services::Memory;
//! use opendal::Operator;
//!
//! #[tokio::test]
//! async fn test() -> Result<()> {
//!     // Build opendal Operator.
//!     let op = Operator::new(Memory::default())?.finish();
//!
//!     // Build fuse3 file system.
//!     let fs = Filesystem::new(op, 1000, 1000);
//!
//!     // Configure mount options.
//!     let mount_options = MountOptions::default();
//!
//!     // Start a fuse3 session and mount it.
//!     let mut mount_handle = Session::new(mount_options)
//!         .mount_with_unprivileged(fs, "/tmp/mount_test")
//!         .await?;
//!     let handle = &mut mount_handle;
//!
//!     tokio::select! {
//!         res = handle => res?,
//!         _ = tokio::signal::ctrl_c() => {
//!             mount_handle.unmount().await?
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

mod file;
mod file_system;
pub use file_system::Filesystem;
