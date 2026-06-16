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

use std::fmt::Debug;

use crate::raw::*;
use crate::*;

/// Layer intercepts an operator's composed runtime resources.
///
/// A layer receives the current stack for each plane and returns the stack that
/// should be used by operators built with this layer. Implementations can wrap
/// the operation service, HTTP transport, task executor, or any combination of
/// them.
///
/// Hooks take `&self`, so layers that keep mutable state must use interior
/// mutability. That state must remain `Send` and `Sync` because layers are
/// shared across cloned operators and concurrent operations.
pub trait Layer: Send + Sync + Debug + Unpin + 'static {
    /// Intercept the operation service stack.
    ///
    /// Operation layers should return a service that forwards unchanged
    /// operations to `inner`.
    fn apply_service(&self, srv: Servicer) -> Servicer {
        srv
    }

    /// Intercept the HTTP transport stack.
    ///
    /// Return `inner` unchanged for layers that do not affect HTTP requests.
    fn apply_http_transport(&self, srv: Servicer, inner: HttpTransporter) -> HttpTransporter {
        let _ = srv;
        inner
    }

    /// Intercept the task execution stack.
    ///
    /// Return `inner` unchanged for layers that do not affect spawned tasks.
    fn apply_execute(&self, srv: Servicer, inner: Executor) -> Executor {
        let _ = srv;
        inner
    }
}
