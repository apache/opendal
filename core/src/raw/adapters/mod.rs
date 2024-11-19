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

//! Providing adapters and its implementations.
//!
//! Adapters in OpenDAL means services that shares similar behaviors. We use
//! adapter to make those services been implemented more easily. For example,
//! with [`kv::Adapter`], users only need to implement `get`, `set` for a service.
//!
//! # Notes
//!
//! Please import the module instead of its type.
//!
//! For example, use the following:
//!
//! ```ignore
//! use opendal::adapters::kv;
//!
//! impl kv::Adapter for MyType {}
//! ```
//!
//! Instead of:
//!
//! ```ignore
//! use opendal::adapters::kv::Adapter;
//!
//! impl Adapter for MyType {}
//! ```
//!
//! # Available Adapters
//!
//! - [`kv::Adapter`]: Adapter for Key Value Services like `redis`.
//! - [`typed_kv::Adapter`]: Adapter key value services that in-memory.

pub mod kv;
pub mod typed_kv;
