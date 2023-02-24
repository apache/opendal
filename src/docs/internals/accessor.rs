// Copyright 2023 Datafuse Labs.
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

//! The internal implement details of [`Accessor`].
//!
//! [`Accessor`] is the core trait of OpenDAL's raw API. We operate
//! underlying storage services via APIs provided by [`Accessor`].
//!
//! [`Accessor`] can be split in the following parts:
//!
//! ```ignore
//! // Attributes
//! #[async_trait]
//! //                  <----------Trait Bound-------------->
//! pub trait Accessor: Send + Sync + Debug + Unpin + 'static {
//!     type Reader: output::Read;                 // --+
//!     type BlockingReader: output::BlockingRead; //   +--> Associated Type
//!     type Pager: output::Page;                  //   +
//!     type BlockingPager: output::BlockingPage;  // --+
//!
//!     // APIs
//!     async fn hello(&self, path: &str, args: OpCreate) -> Result<RpCreate>;
//!     async fn world(&self, path: &str, args: OpCreate) -> Result<RpCreate>;
//! }
//! ```
//!
//! Let's go deep into [`Accessor`] line by line.
//!
//!
//!
//! ## Async Trait
//!
//! At the first line of [`Accessor`], we will read:
//!
//! ```ignore
//! #[async_trait]
//! ```
//!
//! This is an attribute from [`async_trait`](https://docs.rs/async-trait/latest/async_trait/). By using this attribute, we can write the following code without use nightly feature.
//!
//! ```ignore
//! pub trait Accessor {
//!     async fn create(&self, path: &str) -> Result<()>;
//! }
//! ```
//!
//! `async_trait` will transform the `async fn` into:
//!
//! ```ignore
//! pub trait Accessor {
//!     fn create<'async>(
//!         &'async self,
//!     ) -> Pin<Box<dyn core::future::Future<Output = ()> + Send + 'async>>
//!     where Self: Sync + 'async;
//! }
//! ```
//!
//! It's not zero cost, and we will improve this part once the related features are stabilised.
//!
//! ## Trait Bound
//!
//! Then we will read the declare of [`Accessor`] trait:
//!
//! ```ignore
//! pub trait Accessor: Send + Sync + Debug + Unpin + 'static {}
//! ```
//!
//! There are many trait boundings here. For now, [`Accessor`] requires the following bound:
//!
//! - [`Send`]: Allow user to send between threads without extra wrapper.
//! - [`Sync`]: Allow user to sync between threads without extra lock.
//! - [`Debug`][std::fmt::Debug]: Allow users to print underlying debug information of accessor.
//! - [`Unpin`]: Make sure `Accessor` can be safely moved after being pinned, so users don't need to `Pin<Box<A>>`.
//! - `'static`: Make sure `Accessor` is not a short-time reference, allow users to use `Accessor` in clouse, futures without playing with lifetime.
//!
//! Implementer of `Accessor` should take care the following things:
//!
//! - Implement `Debug` for backend, but don't leak creadentails.
//! - Make sure the backend is `Send` and `Sync`, wrap the internal strcut with `Arc<Mutex<T>>` if necessary.
//!
//! ## Associated Type
//!
//! The first block of [`Accessor`] trait is our associated types. We
//! require implementers to specify the type to be returned, thus avoiding
//! the additional overhead of dynamic dispatch.
//!
//! [`Accessor`] has four associated type so far:
//!
//! - `Reader`: reader returned by `read` operation.
//! - `BlockingReader`: reader returned by `blocking_read` operation.
//! - `Pager`: pager returned by `scan` or `list` operation.
//! - `BlockingPager`: pager returned by `blocking_scan` or `blocking_list` operation.
//!
//! Implementer of `Accessor` should take care the following things:
//!
//! - OpenDAL will erase those type at the final stage of Operator building. Please don't return dynamic trait object like `output::Reader`.
//! - Use `()` as type if the operation is not supported.
//!
//! ## API Style
//!
//! Every API of [`Accessor`] follows the same style:
//!
//! - All APIs have a unique [`Operation`] and [`AccessorCapability`]
//! - All APIs are orthogonal and do not overlap with each other
//! - Most APIs accept `path` and `OpXxx`, and returns `RpXxx`.
//! - Most APIs have `async` and `blocking` variants, they share the same semantics but may have different underlying implementations.
//!
//! [`Accessor`] can declare their capabilities via [`AccessorMetadata`]'s `set_capabilities`:
//!
//! ```ignore
//! impl Accessor for MyBackend {
//!     fn metadata(&self) -> AccessorMetadata {
//!        use AccessorCapability::*;
//!
//!        let mut am = AccessorMetadata::default();
//!        am.set_capabilities(Read | Write | List | Scan | Presign | Multipart | Batch);
//!
//!         am
//!     }
//! }
//! ```
//!
//! Now, you have master [`Accessor`], let's go to implement ourselves backend!
//!
//! [`Accessor`]: crate::raw::Accessor
//! [`Operation`]: crate::raw::Operation
//! [`AccessorCapability`]: crate::raw::AccessorCapability
//! [`AccessorMetadata`]: crate::raw::AccessorMetadata
