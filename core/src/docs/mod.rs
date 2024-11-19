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

//! This module holds documentation for OpenDAL.
//!
//! It's highly recommended that you start by reading [`concepts`] first.

#![allow(rustdoc::bare_urls)]

pub mod comparisons;

pub mod concepts;

pub mod internals;

/// Changes log for all OpenDAL released versions.
#[doc = include_str!("../../CHANGELOG.md")]
#[cfg(not(doctest))]
pub mod changelog {}

#[cfg(not(doctest))]
pub mod rfcs;

/// Upgrade and migrate procedures while OpenDAL meets breaking changes.
#[doc = include_str!("upgrade.md")]
#[cfg(not(doctest))]
pub mod upgrade {}
