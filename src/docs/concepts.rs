// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The core concepts of OpenDAL's public API.
//!
//! OpenDAL provides a unified abstraction for all storage services.
//!
//! There are three core concepts in OpenDAL:
//!
//! - [`Builder`]: Build an instance of underlying services.
//! - [`Operator`]: A bridge between underlying implementation detail and  unified abstraction.
//! - [`Object`]: The smallest unit representing a file/dir/... with path in specified services.
//!
//! If you are interested in internal implementation details, please have a look at [`internals`][super::internals].
//!
//! # Builder
//!
//! Let's start with [`Builder`].
//!
//! A `Builder` is a trait that is implemented by the underlying services. We can use a `Builder` to configure and create a service. Builder is the only public API provided by services, and the detailed implementation is hidden.
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
//!
//! The [`Operator`] is a bridge between the underlying implementation details and the unified abstraction. OpenDAL will erase all generic types and higher abstraction around it.
//!
//! ```text
//! ┌───────────┐           ┌───────────┐              ┌─────────────────┐
//! │           │  build()  │           │  type erase  │                 │
//! │  Builder  ├──────────►│  Service  ├─────────────►│     Operator    │
//! │           │           │           │              │                 │
//! └───────────┘           └───────────┘              └─────────────────┘
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
//! let op = Operator::create(builder)?.finish();
//! # Ok(())
//! # }
//! ```
//!
//! - `Operator` has it's internal `Arc`, so it's **cheap** to clone it.
//! - `Operator` doesn't have generice parameters or lifetimes, so it's **easy** to use it everywhere.
//! - `Operator` implements `Send` and `Sync`, so it's **safe** to send it between threads.
//!
//! # Object
//!
//! [`Object`] is the smallest unit representing a file/dir/... with path in specified services. All actual operations will happen on `Object`:
//!
//!
//! ```text
//!                            ┌────────────┐
//!                     abc    │   Object   ├───► read()
//!                  ┌────────►│ path: abc  │
//!                  │         └────────────┘
//! ┌───────────┐    │
//! │ Operator  │    │   def   ┌────────────┐
//! │ ┌───────┐ ├────┼────────►│   Object   ├───► write()
//! │ │Service│ │    │         │ path: def  │
//! └─┴───────┴─┘    │         └────────────┘
//!                  │
//!                  │   ghi/  ┌────────────┐
//!                  └────────►│   Object   ├───► list()
//!                            │ path: ghi/ │
//!                            └────────────┘
//! ```
//!
//! Every `Object` is coresbonding to a `path` which is related to `Operator`'s root. `Object` is the same as `Operator` which is **cheap**, **easy** and **safe** to use everywhere.
//!
//! We can read data from an object in this way:
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
//! let op = Operator::create(builder)?.finish();
//! let o = op.object("abc");
//! let bs: Vec<u8> = o.read().await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`Builder`]: crate::Builder
//! [`Operator`]: crate::Operator
//! [`Object`]: crate::Object
