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

use std::sync::LazyLock;

/// Shared Tokio runtime used to bridge blocking C callers to async OpenDAL APIs.
///
/// Every blocking C call uses `RUNTIME.block_on(...)` on the calling thread.
/// Calling any blocking OpenDAL C function from inside an existing Tokio
/// runtime thread will panic, because `block_on` cannot be nested.  This is
/// the same constraint as the old `blocking::Operator` API.
pub(crate) static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
});
