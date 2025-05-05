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

#[cfg(feature = "services-azblob")]
pub(crate) mod core;
#[cfg(feature = "services-azblob")]
mod delete;
#[cfg(feature = "services-azblob")]
mod error;
#[cfg(feature = "services-azblob")]
mod lister;
#[cfg(feature = "services-azblob")]
pub(crate) mod writer;

#[cfg(feature = "services-azblob")]
mod backend;
#[cfg(feature = "services-azblob")]
pub use backend::AzblobBuilder as Azblob;

mod config;
pub use config::AzblobConfig;
