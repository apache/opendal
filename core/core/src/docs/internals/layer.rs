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

//! The internal implementation details of [`Layer`].
//!
//! OpenDAL has one layer composition surface:
//!
//! - [`Layer`] receives an already erased [`ServiceDyn`] stack and can wrap the
//!   service, HTTP fetcher, or executor.
//! - Layer wrappers still implement typed [`Service`], so their own reader,
//!   writer, lister, deleter, and copier bodies stay concrete.
//! - The composition boundary is [`Servicer`]. Boxing operation
//!   bodies remains centralized in the blanket [`ServiceDyn`] implementation
//!   for typed [`Service`] values.
//!
//! [`Layer`] itself is the runtime hook surface. Every hook returns `inner` by
//! default, so a layer only implements the resource planes it needs:
//!
//! ```ignore
//! pub trait Layer: Send + Sync + Debug + Unpin + 'static {
//!     fn apply_service(&self, srv: Servicer) -> Servicer;
//!     fn apply_http_fetch(&self, srv: Servicer, inner: HttpFetcher) -> HttpFetcher;
//!     fn apply_execute(&self, srv: Servicer, inner: Executor) -> Executor;
//! }
//! ```
//!
//! [`Operator`] replays the same ordered layer list for each resource plane:
//! first the operation service stack, then the HTTP fetch stack and task
//! executor. Resource hooks receive the final [`Servicer`], so they can observe
//! service identity without smuggling it through request extensions. The
//! composed HTTP fetcher and executor are then stored in [`OperationContext`]
//! and passed to service operations.
//!
//! An operation layer normally has two parts:
//!
//! - `XxxLayer` implements [`Layer`] and returns a typed service wrapper from
//!   `apply_service`.
//! - `XxxService` stores the inner [`Servicer`] and implements [`Service`].
//!
//! ```ignore
//! pub struct XxxLayer;
//!
//! impl Layer for XxxLayer {
//!     fn apply_service(&self, inner: Servicer) -> Servicer {
//!         Arc::new(XxxService { inner })
//!     }
//! }
//! ```
//!
//! Most operation layers only override the operations they need and forward the
//! rest to `inner`. This works because [`Servicer`] implements [`Service`] by
//! forwarding calls to [`ServiceDyn`], while the wrapper keeps concrete
//! operation body types until it is returned as a [`Servicer`].
//!
//! Resource-only layers can implement only `apply_http_fetch` or
//! `apply_execute`. If they do not need the final service stack, they should
//! name that parameter `_srv`. Layers that need consistent policy across planes
//! can implement multiple hooks, for example operation and HTTP concurrency
//! limits or operation and I/O timeout handling.
//!
//! [`Layer`]: crate::raw::Layer
//! [`Service`]: crate::raw::Service
//! [`ServiceDyn`]: crate::raw::ServiceDyn
//! [`Servicer`]: crate::raw::Servicer
//! [`HttpFetcher`]: crate::raw::HttpFetcher
//! [`Executor`]: crate::Executor
//! [`OperationContext`]: crate::raw::OperationContext
//! [`Operator`]: crate::Operator
