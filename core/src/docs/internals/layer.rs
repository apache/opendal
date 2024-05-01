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

//! The internal implementation details of [`Layer`].
//!
//! [`Layer`] itself is quite simple:
//!
//! ```ignore
//! pub trait Layer<A: Access> {
//!     type LayeredAccess: Accessor;
//!
//!     fn layer(&self, inner: A) -> Self::LayeredAccess;
//! }
//! ```
//!
//! `XxxLayer` will wrap input [`Access`] as inner and return a new [`Access`]. So normally the implementation of [`Layer`] will be split into two parts:
//!
//! - `XxxLayer` will implement [`Layer`] and return `XxxAccessor` as `Self::LayeredAccess`.
//! - `XxxAccessor` will implement [`Access`] and be built by `XxxLayer`.
//!
//! Most layer only implements part of [`Access`], so we provide
//! [`LayeredAccess`] which will forward all unimplemented methods to
//! `inner`. It's highly recommend to implement [`LayeredAccess`] trait
//! instead.
//!
//! [`Layer`]: crate::raw::Layer
//! [`Access`]: crate::raw::Access
//! [`LayeredAccess`]: crate::raw::LayeredAccess
