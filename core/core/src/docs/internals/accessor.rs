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

//! The internal implementation details of [`Service`].
//!
//! [`Service`] is the core trait of OpenDAL's raw API. We operate
//! underlying storage services via APIs provided by [`Service`].
//!
//! # Introduction
//!
//! [`Service`] can be split in the following parts:
//!
//! ```ignore
//! //                  <----------Trait Bound-------------->
//! pub trait Service: Send + Sync + Debug + Unpin + 'static {
//!     fn info(&self) -> ServiceInfo;
//!     fn capability(&self) -> Capability;
//!
//!     type Reader: oio::Read;
//!     type Writer: oio::Write;
//!     type Lister: oio::List;
//!     type Deleter: oio::Delete;
//!     type Copier: oio::Copy;
//!
//!     fn read(
//!         &self,
//!         ctx: &OperationContext,
//!         path: &str,
//!         args: OpRead,
//!     ) -> impl Future<Output = Result<Self::Reader>> + MaybeSend;
//! }
//! ```
//!
//! Let's go deep into [`Service`] line by line.
//!
//! ## Trait Bound
//!
//! First we will read the declaration of [`Service`] trait:
//!
//! ```ignore
//! pub trait Service: Send + Sync + Debug + Unpin + 'static {}
//! ```
//!
//! There are many trait boundings here. For now, [`Service`] requires the following bound:
//!
//! - [`Send`]: Allow user to send between threads without extra wrapper.
//! - [`Sync`]: Allow user to sync between threads without extra lock.
//! - [`Debug`][std::fmt::Debug]: Allow users to print underlying debug information of service.
//! - [`Unpin`]: Make sure `Service` can be safely moved after being pinned, so users don't need to `Pin<Box<A>>`.
//! - `'static`: Make sure `Service` is not a short-time reference, allow users to use `Service` in closures and futures without playing with lifetime.
//!
//! Implementer of `Service` should take care of the following things:
//!
//! - Implement `Debug` for backend, but don't leak credentials.
//! - Make sure the backend is `Send` and `Sync`, wrap the internal struct with `Arc<Mutex<T>>` if necessary.
//!
//! ## Associated Type
//!
//! The middle block of [`Service`] is our associated types. We require
//! implementers to specify concrete operation body types, avoiding extra
//! dynamic dispatch inside the backend and typed service wrappers.
//!
//! [`Service`] has five associated types so far:
//!
//! - `Reader`: reader returned by `read` operation.
//! - `Writer`: writer returned by `write` operation.
//! - `Lister`: lister returned by `list` operation.
//! - `Deleter`: deleter returned by `delete` operation.
//! - `Copier`: copier returned by `copy` operation.
//!
//! Backend implementers should take care of the following things:
//!
//! - Return concrete operation body types instead of dynamic trait objects like `oio::Reader`.
//! - Use `()` as the type if the operation is not supported.
//! - Implement every operation method; unsupported operations should return
//!   [`ErrorKind::Unsupported`] explicitly.
//!
//! Runtime service wrappers that already receive a [`Servicer`] can forward the
//! erased `oio::*` body types, but the original backend should not erase itself.
//!
//! OpenDAL erases these associated types at the [`ServiceDyn`] boundary.
//! [`Servicer`] is `Arc<dyn ServiceDyn>` and is the handle used by
//! [`Operator`] and runtime layer composition. This keeps backend and typed
//! layer implementations concrete, while the composed operator stack can be
//! stored and cloned through one object-safe service handle.
//!
//! ## API Style
//!
//! Every API of [`Service`] follows the same style:
//!
//! - All APIs have a unique [`Operation`] and [`Capability`]
//! - All APIs are orthogonal and do not overlap with each other
//! - Most APIs accept [`OperationContext`], `path` and `OpXxx`, and returns `RpXxx` with concrete operation bodies.
//! - Most APIs have `async` and `blocking` variants, they share the same semantics but may have different underlying implementations.
//!
//! [`OperationContext`] carries operator resources composed by operator layers,
//! such as HTTP client and executor. Backend implementations should use this
//! context instead of storing mutable operator resources in [`ServiceInfo`].
//!
//! [`Service`] declares immutable identity facts via [`ServiceInfo`] and
//! operation capability via [`Service::capability`]:
//!
//! ```ignore
//! impl Service for MyBackend {
//!     type Reader = MyReader;
//!     type Writer = ();
//!     type Lister = ();
//!     type Deleter = ();
//!     type Copier = ();
//!
//!     fn info(&self) -> ServiceInfo {
//!         ServiceInfo::new(MY_SCHEME, "/", "")
//!     }
//!
//!     fn capability(&self) -> Capability {
//!         Capability {
//!             read: true,
//!             write: true,
//!             ..Default::default()
//!         }
//!     }
//! }
//! ```
//!
//! Now that you have mastered [`Service`], let's go and implement our own backend!
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
//! /// use opendal_core::services::Duck;
//! /// use opendal_core::Operator;
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
//! ///     let op: Operator = Operator::new(builder)?;
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
//!     fn build(self) -> Result<impl Service>  {
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
//! `DuckBackend` that implements [`Service`]. The backend is what we used
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
//! impl Service for DuckBackend {
//!     type Reader = DuckReader;
//!     type Writer = ();
//!     type Lister = ();
//!     type Deleter = ();
//!     type Copier = ();
//!
//!     fn info(&self) -> ServiceInfo {
//!         ServiceInfo::new(DUCK_SCHEME, &self.root, "")
//!     }
//!
//!     fn capability(&self) -> Capability {
//!         Capability {
//!             read: true,
//!             ..Default::default()
//!         }
//!     }
//!
//!     fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
//!         gagaga!()
//!     }
//! }
//! ```
//!
//! Congratulations, we have implemented a [`Service`] that can talk to
//! Super Power Ducks!
//!
//! What!? There are no Super Power Ducks? So sad, but never mind, we have
//! really powerful storage services [here](https://github.com/apache/opendal/issues/5). Welcome to pick one to implement. I promise you won't
//! have to `gagaga!()` this time.
//!
//! [`Service`]: crate::raw::Service
//! [`ServiceDyn`]: crate::raw::ServiceDyn
//! [`Servicer`]: crate::raw::Servicer
//! [`Operation`]: crate::raw::Operation
//! [`Capability`]: crate::Capability
//! [`ServiceInfo`]: crate::raw::ServiceInfo
//! [`OperationContext`]: crate::raw::OperationContext
//! [`Operator`]: crate::Operator
//! [`Builder`]: crate::Builder
//! [`Configurator`]: crate::Configurator
