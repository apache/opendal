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

//! The internal implement details of OpenDAL.
//!
//! OpenDAL has provides unified abstraction via two-level API sets:
//!
//! - Public API like [`Operator`] provides user level API.
//! - Raw API like [`Access`], [`Layer`] provides developer level API.
//!
//! OpenDAL tries it's best to keep the public API stable. But raw APIs
//! may change between minor releases from time to time. So most users
//! should only use the public API. And only developers need to implement
//! with raw API to implement a new service [`Access`] or their own
//! [`Layer`].
//!
//! In this section, we will talk about the following components:
//!
//! - [`Access`][accessor]: to connect underlying storage services.
//! - [`Layer`][layer]: middleware/interceptor between storage services.
//!
//! The relation between [`Access`], [`Layer`] and [`Operator`] looks like the following:
//!
//! ```text
//! ┌─────────────────────────────────────────────────┬──────────┐
//! │                                                 │          │
//! │              ┌──────────┐  ┌────────┐           │          │
//! │              │          │  │        ▼           │          │
//! │      s3──┐   │          │  │ Tracing Layer      │          │
//! │          │   │          │  │        │           │          │
//! │     gcs──┤   │          │  │        ▼           │          │
//! │          ├──►│ Accessor ├──┘ Metrics Layer ┌───►│ Operator │
//! │  azblob──┤   │          │           │      │    │          │
//! │          │   │          │           ▼      │    │          │
//! │    hdfs──┘   │          │    Logging Layer │    │          │
//! │              │          │           │      │    │          │
//! │              └──────────┘           └──────┘    │          │
//! │                                                 │          │
//! └─────────────────────────────────────────────────┴──────────┘
//! ```
//!
//! [`Builder`]: crate::Builder
//! [`Operator`]: crate::Operator
//! [`Access`]: crate::raw::Access
//! [`Layer`]: crate::raw::Layer

pub mod accessor;
pub mod layer;
