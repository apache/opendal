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

/// Default scheme for swift service.
#[cfg(feature = "services-swift")]
pub(super) const DEFAULT_SCHEME: &str = "swift";
#[cfg(feature = "services-swift")]
mod core;
#[cfg(feature = "services-swift")]
mod delete;
#[cfg(feature = "services-swift")]
mod error;
#[cfg(feature = "services-swift")]
mod lister;
#[cfg(feature = "services-swift")]
mod writer;

#[cfg(feature = "services-swift")]
mod backend;
#[cfg(feature = "services-swift")]
pub use backend::SwiftBuilder as Swift;

mod config;
pub use config::SwiftConfig;
