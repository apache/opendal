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
//!   service or its [`OperationContext`].
//! - Layer wrappers still implement typed [`Service`], so their own reader,
//!   writer, lister, deleter, and copier bodies stay concrete.
//! - The composition boundary is [`Servicer`]. Boxing operation
//!   bodies remains centralized in the blanket [`ServiceDyn`] implementation
//!   for typed [`Service`] values.
//!
//! [`Layer`] itself is the runtime hook surface. Every hook returns `inner` by
//! default, so a layer only implements the plane it needs:
//!
//! ```ignore
//! pub trait Layer: Send + Sync + Debug + Unpin + 'static {
//!     fn apply_service(&self, srv: Servicer) -> Servicer;
//!     fn apply_context(&self, srv: Servicer, inner: OperationContext) -> OperationContext;
//! }
//! ```
//!
//! [`Operator`] replays the same ordered layer list over the base service and
//! base [`OperationContext`]. It first composes the operation service stack.
//! Then context hooks receive the final [`Servicer`] and compose runtime
//! resources such as HTTP transport and executor. The composed context is passed
//! to service operations.
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
//! Resource-only layers can implement only `apply_context`. If they do not need
//! the final service stack, they should name that parameter `_srv`.
//!
//! ```ignore
//! pub struct TransportLayer {
//!     transport: HttpTransporter,
//! }
//!
//! impl Layer for TransportLayer {
//!     fn apply_context(&self, _srv: Servicer, inner: OperationContext) -> OperationContext {
//!         inner.with_http_transport(self.transport.clone())
//!     }
//! }
//! ```
//!
//! Layers that need consistent policy across planes can implement both hooks,
//! for example operation and I/O timeout handling. Resource wrappers that
//! replace HTTP transport or executor must forward to the previous value when
//! they want lower layers to remain effective.
//!
//! [`Layer`]: crate::raw::Layer
//! [`Service`]: crate::raw::Service
//! [`ServiceDyn`]: crate::raw::ServiceDyn
//! [`Servicer`]: crate::raw::Servicer
//! [`HttpTransporter`]: crate::HttpTransporter
//! [`Executor`]: crate::Executor
//! [`OperationContext`]: crate::raw::OperationContext
//! [`Operator`]: crate::Operator
