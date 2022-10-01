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

use crate::ObjectEntry;
use futures::Stream;
use futures::StreamExt;
use std::io::Result;
use std::pin::Pin;
use std::task::Poll;
use std::task::{ready, Context};
use std::vec::IntoIter;

/// ObjectStream represents a stream of Object.
pub trait ObjectStream: Stream<Item = Result<ObjectEntry>> + Unpin + Send {}
impl<T> ObjectStream for T where T: Stream<Item = Result<ObjectEntry>> + Unpin + Send {}

/// ObjectStreamer is a boxed dyn [`ObjectStream`]
pub type ObjectStreamer = Box<dyn ObjectStream>;

/// EmptyObjectStreamer that always return None.
pub struct EmptyObjectStreamer;

impl Stream for EmptyObjectStreamer {
    type Item = Result<ObjectEntry>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// ObjectPageStream represents a stream of Object Page which contains a vector
/// of [`ObjectEntry`].
pub trait ObjectPageStream: Stream<Item = Result<Vec<ObjectEntry>>> + Unpin + Send {}
impl<T> ObjectPageStream for T where T: Stream<Item = Result<Vec<ObjectEntry>>> + Unpin + Send {}

/// ObjectPageStreamer will convert an [`ObjectPageStream`] to [`ObjectStream`]
pub struct ObjectPageStreamer<S: ObjectPageStream> {
    inner: S,
    entries: IntoIter<ObjectEntry>,
}

impl<S> ObjectPageStreamer<S>
where
    S: ObjectPageStream,
{
    /// Create a new ObjectPageStreamer.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            entries: vec![].into_iter(),
        }
    }
}

impl<S> Stream for ObjectPageStreamer<S>
where
    S: ObjectPageStream,
{
    type Item = Result<ObjectEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Try to fetch entry from already cached entries.
        if let Some(entry) = self.entries.next() {
            return Poll::Ready(Some(Ok(entry)));
        }
        // If all entries have been consumed, try to fetch next page.
        match ready!(self.inner.poll_next_unpin(cx)) {
            None => Poll::Ready(None),
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            Some(Ok(entries)) => {
                self.entries = entries.into_iter();
                self.poll_next(cx)
            }
        }
    }
}
