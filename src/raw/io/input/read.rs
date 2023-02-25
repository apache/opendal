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

/// Reader is a boxed dyn of [`Read`];
///
/// We use [`Reader`] to accept users input in `Accessor` trait.
pub type Reader = Box<dyn Read>;

/// Read is a trait alias of [`futures::AsyncRead`] to avoid repeating
/// `futures::AsyncRead + Unpin + Send` across the codebase.
///
/// We use [`Read`] to accept users input.
pub trait Read: futures::AsyncRead + Unpin + Send {}
impl<T> Read for T where T: futures::AsyncRead + Unpin + Send {}
