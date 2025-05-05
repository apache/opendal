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

//! OpenDAL Performance Guide
//!
//! OpenDAL is designed in a zero-cost abstraction way, which means that
//! users won't pay for the abstraction cost if they don't use it. But this
//! can also means that users can't maximize the performance of OpenDAL
//! if they don't know how to use it.
//!
//! This document will introduce some tips to improve the performance of
//! OpenDAL.

#[doc = include_str!("concurrent_write.md")]
pub mod concurrent_write {}

#[doc = include_str!("http_optimization.md")]
pub mod http_optimization {}
