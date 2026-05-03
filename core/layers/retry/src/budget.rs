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

//! Retry budget primitives.
//!
//! A retry budget caps how many retries the layer is allowed to issue relative to recent
//! operations. This prevents retry storms from amplifying load on a degraded backend.

use std::fmt::Debug;

use opendal_core::raw::Operation;

/// Controls whether a retry attempt is allowed.
pub trait RetryBudget: Debug + Send + Sync + 'static {
    /// Record a successful operation. Grows the budget.
    fn deposit(&self, op: Operation);

    /// Try to consume one retry token. Returns `false` if no budget remains.
    fn withdraw(&self, op: Operation) -> bool;
}

/// A no-op budget that always allows retries.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnlimitedBudget;

impl RetryBudget for UnlimitedBudget {
    fn deposit(&self, _op: Operation) {}

    fn withdraw(&self, _op: Operation) -> bool {
        true
    }
}
