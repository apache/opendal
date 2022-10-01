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

use std::io::Result;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::ObjectEntry;

/// ObjectStream represents a stream of Dir.
pub trait ObjectStream: futures::Stream<Item = Result<ObjectEntry>> + Unpin + Send {}
impl<T> ObjectStream for T where T: futures::Stream<Item = Result<ObjectEntry>> + Unpin + Send {}

/// ObjectStreamer is a boxed dyn [`ObjectStream`]
pub type ObjectStreamer = Box<dyn ObjectStream>;

/// EmptyObjectStreamer that always return None.
pub(crate) struct EmptyObjectStreamer;

impl futures::Stream for EmptyObjectStreamer {
    type Item = Result<ObjectEntry>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
