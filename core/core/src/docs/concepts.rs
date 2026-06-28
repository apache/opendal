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

//! The core concepts of OpenDAL's public API.
//!
//! OpenDAL provides a unified abstraction for accessing storage services.
//!
//! Public users mostly work with three concepts:
//!
//! - [`Builder`]: configures and builds a storage service.
//! - [`Operator`]: owns a service stack and exposes storage operations such as
//!   `read`, `write`, and `list`.
//! - [`Layer`][crate::raw::Layer]: wraps an operator's service stack or runtime
//!   resources to add behavior such as retry, timeout, tracing, and metrics.
//!
//! If you are interested in internal implementation details, please have a look at [`internals`][super::internals].
//!
//! # Builder
//!
//! A [`Builder`] configures and creates the underlying storage service. Service
//! crates expose builders through [`services`][crate::services], for example
//! [`services::Memory`][crate::services::Memory].
//!
//! ```text
//! ┌─────────┐   build()   ┌─────────┐
//! │ Builder ├────────────►│ Service │
//! └─────────┘             └─────────┘
//! ```
//!
//! ```no_run
//! use opendal_core::services::Memory;
//!
//! let builder = Memory::default();
//! ```
//!
//! # Operator
//!
//! An [`Operator`] is the public handle for a storage service stack. It stores:
//!
//! - a base service created from the builder.
//! - a base [`OperationContext`] with runtime resources such as HTTP transport
//!   and executor.
//! - an ordered list of layers.
//! - the composed service and context used by storage operations.
//!
//! ```text
//! ┌─────────┐   Operator::new   ┌────────────────────────────┐
//! │ Builder ├──────────────────►│ Operator                   │
//! └─────────┘                   │ - base service             │
//!                               │ - base OperationContext    │
//!                               │ - layers                   │
//!                               │ - composed service/context │
//!                               └────────────────────────────┘
//! ```
//!
//! `Operator::new` returns a ready-to-use operator. There is no separate
//! `finish` step.
//!
//! ```no_run
//! # use opendal_core::Result;
//! use opendal_core::services::Memory;
//! use opendal_core::Operator;
//!
//! # fn test() -> Result<()> {
//! let builder = Memory::default();
//!
//! let op = Operator::new(builder)?;
//! # Ok(())
//! # }
//! ```
//!
//! - `Operator` is cheap to clone because it stores shared handles internally.
//! - `Operator` has no generic parameters or lifetimes, so it is easy to pass
//!   through application code.
//! - `Operator` is `Send` and `Sync`, so it can be shared across threads.
//! - Methods that change layers or runtime resources return a new operator;
//!   existing clones and in-flight operations keep using their current composed
//!   service and context.
//!
//! # Runtime resources and layers
//!
//! [`OperationContext`] carries runtime resources from the operator to services,
//! such as [`HttpTransporter`][crate::HttpTransporter] and [`Executor`][crate::Executor].
//! Operation arguments such as ranges, versions, and concurrency limits stay in
//! the `Op*` argument structs used by each operation.
//!
//! Use [`Operator::with_context`] to replace the base context. Use
//! [`Operator::layer`] to append a layer. In both cases, OpenDAL replays the full
//! layer list from the base service and base context to produce a fresh composed
//! service and context.
//!
//! ```text
//! base service ─┐
//!               ├─ layer replay ─► composed service ─┐
//! base context ─┘                                     ├─► operation dispatch
//!                                                     │    srv.read(&ctx, ...)
//!                                                     ▼
//!                                             composed context
//! ```
//!
//! ```no_run
//! # use opendal_core::Result;
//! use opendal_core::HttpTransporter;
//! use opendal_core::OperationContext;
//! use opendal_core::Operator;
//! use opendal_core::services::Memory;
//!
//! # fn test() -> Result<()> {
//! let transport = HttpTransporter::default();
//! let op = Operator::new(Memory::default())?.with_context(
//!     OperationContext::new().with_http_transport(transport),
//! );
//! # let _ = op;
//! # Ok(())
//! # }
//! ```
//!
//! # Operations
//!
//! After creating an operator, use it to run operations on normalized paths.
//!
//! ```text
//!                            ┌──────────────┐
//!                  ┌────────►│ read("abc")  │
//!                  │         └──────────────┘
//! ┌───────────┐    │
//! │ Operator  │    │         ┌──────────────┐
//! │ ┌───────┐ ├────┼────────►│ write("def") │
//! │ │Service│ │    │         └──────────────┘
//! └─┴───────┴─┘    │
//!                  │         ┌──────────────┐
//!                  └────────►│ list("ghi/") │
//!                            └──────────────┘
//! ```
//!
//! We can read data with given path in this way:
//!
//! ```no_run
//! # use opendal_core::Result;
//! use opendal_core::services::Memory;
//! use opendal_core::Operator;
//!
//! # async fn test() -> Result<()> {
//! let builder = Memory::default();
//!
//! let op = Operator::new(builder)?;
//! let bs: Vec<u8> = op.read("abc").await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`Builder`]: crate::Builder
//! [`Operator`]: crate::Operator
//! [`OperationContext`]: crate::OperationContext
