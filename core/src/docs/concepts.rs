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

//! The core concepts of OpenDAL's public API.
//!
//! OpenDAL provides a unified abstraction that helps developers access all storage services.
//!
//! There are two core concepts in OpenDAL:
//!
//! - [`Builder`]: Builder accepts a series of parameters to set up an instance of underlying services.
//! You can adjust the behaviour of underlying services with these parameters.
//! - [`Operator`]: Developer can access underlying storage services with manipulating one Operator.
//! The Operator is a delegate for underlying implementation detail, and provides one unified access interface,
//! including `read`, `write`, `list` and so on.
//!
//! If you are interested in internal implementation details, please have a look at [`internals`][super::internals].
//!
//! # Builder
//!
//! Let's start with [`Builder`].
//!
//! A `Builder` is a trait that is implemented by the underlying services. We can use a `Builder` to configure and create a service.
//! Developer can only create one service via Builder, in other words, Builder is the only public API provided by services.
//! And other detailed implementation will be hidden.
//!
//! ```text
//! ┌───────────┐                 ┌───────────┐
//! │           │     build()     │           │
//! │  Builder  ├────────────────►│  Service  │
//! │           │                 │           │
//! └───────────┘                 └───────────┘
//! ```
//!
//! All [`Builder`] provided by OpenDAL is under [`services`][crate::services], we can refer to them like `opendal::services::S3`.
//! By right the builder will be named like `OneServiceBuilder`, but usually we will export it to public with renaming it as one
//! general name. For example, we will rename `S3Builder` to `S3` and developer will use `S3` finally.
//!
//! For example:
//!
//! ```no_run
//! use opendal::services::S3;
//!
//! let mut builder = S3::default();
//! builder.bucket("example");
//! builder.root("/path/to/file");
//! ```
//!
//! # Operator
//! The [`Operator`] is a delegate for Service, the underlying implementation detail that implements [`Access`][crate::raw::Access],
//! and it also provides one unified access interface.
//! It will hold one reference of Service with its all generic types erased by OpenDAL,
//! which is the reason why we say the Operator is the delegate of one Service.
//!
//! ```text
//!                   ┌────────────────────┐
//!                   │      Operator      │
//!                   │         │delegate  │
//! ┌─────────┐ build │         ▼          │ rely on ┌─────────────────────┐
//! │ Builder ├───────┼──►┌────────────┐   │◄────────┤ business logic code │
//! └─────────┘       │   │  Service   │   │         └─────────────────────┘
//!                   └───┴────────────┴───┘
//! ```
//!
//! `Operator` can be built from `Builder`:
//!
//! ```no_run
//! # use opendal::Result;
//! use opendal::services::S3;
//! use opendal::Operator;
//!
//! # fn test() -> Result<()> {
//! let mut builder = S3::default();
//! builder.bucket("example");
//! builder.root("/path/to/file");
//!
//! let op = Operator::new(builder)?.finish();
//! # Ok(())
//! # }
//! ```
//!
//! - `Operator` has it's internal `Arc`, so it's **cheap** to clone it.
//! - `Operator` doesn't have generic parameters or lifetimes, so it's **easy** to use it everywhere.
//! - `Operator` implements `Send` and `Sync`, so it's **safe** to send it between threads.
//!
//! After get an `Operator`, we can do operations on different paths.
//!
//!
//! ```text
//!                            ┌──────────────┐
//!                  ┌────────►│ read("abc")  │
//!                  │         └──────────────┘
//! ┌───────────┐    │
//! │ Operator  │    │         ┌──────────────┐
//! │ ┌───────┐ ├────┼────────►│ write("def") │
//! │ │Service│ │    │         └──────────────┘
//! └─┴───────┴─┘    │
//!                  │         ┌──────────────┐
//!                  └────────►│ list("ghi/") │
//!                            └──────────────┘
//! ```
//!
//! We can read data with given path in this way:
//!
//! ```no_run
//! # use opendal::Result;
//! use opendal::services::S3;
//! use opendal::Operator;
//!
//! # async fn test() -> Result<()> {
//! let mut builder = S3::default();
//! builder.bucket("example");
//! builder.root("/path/to/file");
//!
//! let op = Operator::new(builder)?.finish();
//! let bs: Vec<u8> = op.read("abc").await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`Builder`]: crate::Builder
//! [`Operator`]: crate::Operator
