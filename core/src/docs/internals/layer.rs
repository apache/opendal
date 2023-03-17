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

//! The internal implement details of [`Layer`].
//!
//! [`Layer`] itself is quiet simple:
//!
//! ```ignore
//! pub trait Layer<A: Accessor> {
//!     type LayeredAccessor: Accessor;
//!
//!     fn layer(&self, inner: A) -> Self::LayeredAccessor;
//! }
//! ```
//!
//! `XxxLayer` will wrap input [`Accessor`] as inner and returns a new [`Accessor`]. So normally the implementation of [`Layer`] will be split into two parts:
//!
//! - `XxxLayer` will implements [`Layer`] and return `XxxAccessor` as `Self::LayeredAccessor`.
//! - `XxxAccessor` will implements [`Accessor`] and being built by `XxxLayer`.
//!
//! Most layer only implements part of [`Accessor`], so we provide
//! [`LayeredAccessor`] which will forward all not implemented methods to
//! `inner`. It's highly recommend to implement [`LayeredAccessor`] trait
//! instead.
//!
//! [`Layer`]: crate::raw::Layer
//! [`Accessor`]: crate::raw::Accessor
//! [`LayeredAccessor`]: crate::raw::LayeredAccessor
