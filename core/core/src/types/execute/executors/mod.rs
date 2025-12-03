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

//! executors module provides implementations for the [`Execute`](crate::Execute) trait for widely used runtimes.
//!
//! Every executor will be hide behind the feature like `executors-xxx`. Users can switch or enable
//! the executors they want by enabling the corresponding feature. Also, users can provide their
//! own executor by implementing the [`Execute`](crate::Execute) trait directly.

#[cfg(feature = "executors-tokio")]
mod tokio_executor;
#[cfg(feature = "executors-tokio")]
pub use tokio_executor::TokioExecutor;
