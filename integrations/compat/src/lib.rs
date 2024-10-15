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

//! `opendal-compat` provides compatibility functions for opendal.
//!
//! OpenDAL is widely used across the entire big data ecosystem. Various projects may utilize
//! different versions of OpenDAL. This crate provides compatibility functions to assist users
//! in upgrading OpenDAL without altering their existing code, especially for projects that
//! accept OpenDAL Operators.
//!
//! This project is organized by version. Each version has its own module hidden within a feature,
//! and each module contains only one function that converts from the latest version to the
//! previous version.
//!
//! Currently, `opendal-compat` supports the following versions:
//!
//! - [`v0_50_to_v0_49`]
//!
//! Please refer to the specific function for more information.

#[cfg(feature = "v0_50_to_v0_49")]
mod v0_50_to_v0_49;
#[cfg(feature = "v0_50_to_v0_49")]
pub use v0_50_to_v0_49::v0_50_to_v0_49;
