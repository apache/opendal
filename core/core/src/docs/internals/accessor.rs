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

//! The internal implementation details of [`Access`].
//!
//! [`Access`] is the core trait of OpenDAL's raw API. We operate
//! underlying storage services via APIs provided by [`Access`].
//!
//! # Introduction
//!
//! [`Access`] can be split in the following parts:
//!
//! ```ignore
//! //                  <----------Trait Bound-------------->
//! pub trait Access: Send + Sync + Debug + Unpin + 'static {
//!     type Reader: oio::Read;                    // --+
//!     type Writer: oio::Write;                   //   +--> Associated Type
//!     type Lister: oio::List;                    //   +
//!     type Deleter: oio::Delete;                 // --+
//!
//!     // APIs
//!     fn info(&self) -> Arc<AccessorInfo>;
//!     fn create_dir(
//!         &self,
//!         path: &str,
//!         args: OpCreateDir,
//!     ) -> impl core::future::Future<Output = Result<RpCreateDir>> + MaybeSend;
//! }
//! ```
//!
//! Let's go deep into [`Access`] line by line.
//!
//! ## Trait Bound
//!
//! First we will read the declare of [`Access`] trait:
//!
//! ```ignore
//! pub trait Access: Send + Sync + Debug + Unpin + 'static {}
//! ```
//!
//! There are many trait boundings here. For now, [`Access`] requires the following bound:
//!
//! - [`Send`]: Allow user to send between threads without extra wrapper.
//! - [`Sync`]: Allow user to sync between threads without extra lock.
//! - [`Debug`][std::fmt::Debug]: Allow users to print underlying debug information of accessor.
//! - [`Unpin`]: Make sure `Access` can be safely moved after being pinned, so users don't need to `Pin<Box<A>>`.
//! - `'static`: Make sure `Access` is not a short-time reference, allow users to use `Access` in closures and futures without playing with lifetime.
//!
//! Implementer of `Access` should take care of the following things:
//!
//! - Implement `Debug` for backend, but don't leak credentials.
//! - Make sure the backend is `Send` and `Sync`, wrap the internal struct with `Arc<Mutex<T>>` if necessary.
//!
//! ## Associated Type
//!
//! The first block of [`Access`] trait is our associated types. We
//! require implementers to specify the type to be returned, thus avoiding
//! the additional overhead of dynamic dispatch.
//!
//! [`Access`] has four associated types so far:
//!
//! - `Reader`: reader returned by `read` operation.
//! - `Writer`: writer returned by `write` operation.
//! - `Lister`: lister returned by `list` operation.
//! - `Deleter`: deleter returned by `delete` operation.
//!
//! Implementer of `Access` should take care the following things:
//!
//! - OpenDAL will erase those type at the final stage of Operator building. Please don't return dynamic trait object like `oio::Reader`.
//! - Use `()` as type if the operation is not supported.
//!
//! ## API Style
//!
//! Every API of [`Access`] follows the same style:
//!
//! - All APIs have a unique [`Operation`] and [`Capability`]
//! - All APIs are orthogonal and do not overlap with each other
//! - Most APIs accept `path` and `OpXxx`, and returns `RpXxx`.
//! - Most APIs have `async` and `blocking` variants, they share the same semantics but may have different underlying implementations.
//!
//! [`Access`] can declare their capabilities via [`AccessorInfo`]'s `set_native_capability`:
//!
//! ```ignore
//! impl Access for MyBackend {
//!     fn info(&self) -> Arc<AccessorInfo> {
//!         let am = AccessorInfo::default();
//!         am.set_native_capability(
//!             Capability {
//!                 read: true,
//!                 write: true,
//!                 ..Default::default()
//!         });
//!
//!         am.into()
//!     }
//! }
//! ```
//!
//! Now that you have mastered [`Access`], let's go and implement our own backend!
//!
//! # Tutorial
//!
//! This tutorial implements a `duck` storage service that sends API
//! requests to a super-powered duck. Gagaga!
//!
//! ## Scheme
//!
//! First of all, let's pick a good scheme for our duck service. The
//! scheme should be unique and easy to understand. Normally we should
//! use its formal name.
//!
//! For example, we will use `s3` for AWS S3 Compatible Storage Service
//! instead of `aws` or `awss3`. This is because there are many storage
//! vendors that provide s3-like RESTful APIs, and our s3 service is
//! implemented to support all of them, not just AWS S3.
//!
//! Obviously, we can use `duck` as scheme, let's add a new constant string:
//!
//! ```ignore
//! pub const DUCK_SCHEME: &str = "duck";
//! ```
//!
//! ## Builder
//!
//! Then we can implement a builder for the duck service. The [`Builder`]
//! will provide APIs for users to configure, and they will create an
//! instance of a particular service.
//!
//! Let's create a `backend` mod under `services/duck` directory, and adding the following code.
//!
//! ```ignore
//! use crate::raw::*;
//! use crate::*;
//!
//! /// Duck Storage Service support. Gagaga!
//! ///
//! /// # Capabilities
//! ///
//! /// This service can be used to:
//! ///
//! /// - [x] read
//! /// - [ ] write
//! /// - [ ] list
//! /// - [ ] presign
//! ///
//! /// # Configuration
//! ///
//! /// - `root`: Set the work dir for backend.
//! ///
//! /// ## Via Builder
//! ///
//! /// ```no_run
//! /// use std::sync::Arc;
//! ///
//! /// use anyhow::Result;
//! /// use opendal::services::Duck;
//! /// use opendal::Operator;
//! ///
//! /// #[tokio::main]
//! /// async fn main() -> Result<()> {
//! ///     // Create Duck backend builder.
//! ///     let mut builder = DuckBuilder::default();
//! ///     // Set the root for duck, all operations will happen under this root.
//! ///     //
//! ///     // NOTE: the root must be absolute path.
//! ///     builder.root("/path/to/dir");
//! ///
//! ///     let op: Operator = Operator::new(builder)?.finish();
//! ///
//! ///     Ok(())
//! /// }
//! /// ```
//! #[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
//! #[serde(default)]
//! #[non_exhaustive]
//! pub struct DuckConfig {
//!     pub root: Option<String>,
//! }
//!
//! #[derive(Default, Clone)]
//! pub struct DuckBuilder {
//!     config: DuckConfig,
//! }
//! ```
//!
//! Now let's implement the required APIs for `DuckConfig`:
//!
//! ```ignore
//! impl Configurator for DuckConfig {
//!     type Builder = DuckBuilder;
//!
//!     fn into_builder(self) -> Self::Builder {
//!         DuckBuilder { config: self }
//!     }
//! }
//! ```
//!
//! Note that `DuckBuilder` is part of our public API, so it needs to be
//! documented. And any changes you make will directly affect users, so
//! please take it seriously. Otherwise, you will be hunted down by many
//! angry ducks.
//!
//! Then, we can implement required APIs for `DuckBuilder`:
//!
//! ```ignore
//! impl DuckBuilder {
//!     /// Set root of this backend.
//!     ///
//!     /// All operations will happen under this root.
//!     pub fn root(&mut self, root: &str) -> &mut Self {
//!         self.config.root = if root.is_empty() {
//!             None
//!         } else {
//!             Some(root.to_string())
//!         };
//!
//!         self
//!     }
//! }
//!
//! impl Builder for DuckBuilder {
//!     type Config = DuckConfig;
//!
//!     fn build(self) -> Result<impl Access>  {
//!         debug!("backend build started: {:?}", &self);
//!
//!         let root = normalize_root(&self.config.root.clone().unwrap_or_default());
//!         debug!("backend use root {}", &root);
//!
//!         Ok(DuckBackend { root })
//!     }
//! }
//! ```
//!
//! `DuckBuilder` is ready now, let's try to play with real ducks!
//!
//! ## Backend
//!
//! I'm sure you can see it already: `DuckBuilder` will build a
//! `DuckBackend` that implements [`Access`]. The backend is what we used
//! to communicate with the super-powered ducks!
//!
//! Let's keep adding more code under `backend.rs`:
//!
//! ```ignore
//! /// Duck storage service backend
//! #[derive(Clone, Debug)]
//! pub struct DuckBackend {
//!     root: String,
//! }
//!
//! impl Access for DuckBackend {
//!     type Reader = DuckReader;
//!     type Writer = ();
//!     type Lister = ();
//!     type Deleter = ();
//!
//!     fn info(&self) -> Arc<AccessorInfo> {
//!         let am = AccessorInfo::default();
//!         am.set_scheme(DUCK_SCHEME)
//!             .set_root(&self.root)
//!             .set_native_capability(
//!                 Capability {
//!                     read: true,
//!                     ..Default::default()
//!             });
//!
//!         am.into()
//!     }
//!
//!     async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
//!         gagaga!()
//!     }
//! }
//! ```
//!
//! Congratulations, we have implemented an [`Access`] that can talk to
//! Super Power Ducks!
//!
//! What!? There are no Super Power Ducks? So sad, but never mind, we have
//! really powerful storage services [here](https://github.com/apache/opendal/issues/5). Welcome to pick one to implement. I promise you won't
//! have to `gagaga!()` this time.
//!
//! [`Access`]: crate::raw::Access
//! [`Operation`]: crate::raw::Operation
//! [`Capability`]: crate::Capability
//! [`AccessorInfo`]: crate::raw::AccessorInfo
//! [`Builder`]: crate::Builder
//! [`Configurator`]: crate::Configurator
