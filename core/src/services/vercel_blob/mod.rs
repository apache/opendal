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

/// Default scheme for vercel_blob service.
#[cfg(feature = "services-vercel-blob")]
pub(super) const DEFAULT_SCHEME: &str = "vercel_blob";
#[cfg(feature = "services-vercel-blob")]
mod core;
#[cfg(feature = "services-vercel-blob")]
mod delete;
#[cfg(feature = "services-vercel-blob")]
mod error;
#[cfg(feature = "services-vercel-blob")]
mod lister;
#[cfg(feature = "services-vercel-blob")]
mod writer;

#[cfg(feature = "services-vercel-blob")]
mod backend;
#[cfg(feature = "services-vercel-blob")]
pub use backend::VercelBlobBuilder as VercelBlob;

mod config;
pub use config::VercelBlobConfig;
