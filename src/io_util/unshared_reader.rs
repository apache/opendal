// Copyright 2022 Datafuse Labs.
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

//! This mod is inspired by [async-compression](https://docs.rs/crate/async-compression/0.3.1/source/src/unshared.rs)
//!
//! More discussion could be found at [What shall Sync mean across an .await?](https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020/26)

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::AsyncRead;

use crate::BytesRead;

/// Wraps a type and only allows unique borrowing, the main usecase is to wrap a `!Sync` type and
/// implement `Sync` for it as this type blocks having multiple shared references to the inner
/// value.
///
/// # Safety
///
/// We must be careful when accessing `inner`, there must be no way to create a shared reference to
/// it from a shared reference to an `Unshared`, as that would allow creating shared references on
/// multiple threads.
///
/// As an example deriving or implementing `Clone` is impossible, two threads could attempt to
/// clone a shared `Unshared<T>` reference which would result in accessing the same inner value
/// concurrently.
pub fn unshared_reader<R: BytesRead>(r: R) -> UnsharedReader<R> {
    UnsharedReader::new(r)
}

/// Wraps a type and only allows unique borrowing, the main usecase is to wrap a `!Sync` type and
/// implement `Sync` for it as this type blocks having multiple shared references to the inner
/// value.
///
/// # Safety
///
/// We must be careful when accessing `inner`, there must be no way to create a shared reference to
/// it from a shared reference to an `Unshared`, as that would allow creating shared references on
/// multiple threads.
///
/// As an example deriving or implementing `Clone` is impossible, two threads could attempt to
/// clone a shared `Unshared<T>` reference which would result in accessing the same inner value
/// concurrently.
pub struct UnsharedReader<R: BytesRead>(R);

impl<R: BytesRead> UnsharedReader<R> {
    pub fn new(inner: R) -> Self {
        UnsharedReader(inner)
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.0
    }
}

/// Safety: See comments on main docs for `UnsharedReader`
unsafe impl<R: BytesRead> Sync for UnsharedReader<R> {}

impl<R: BytesRead> AsyncRead for UnsharedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(self.get_mut().get_mut()).poll_read(cx, buf)
    }
}
