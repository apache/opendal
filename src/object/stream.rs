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

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;
use std::vec::IntoIter;

use futures::Stream;
use pin_project::pin_project;

use crate::ObjectEntry;

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
#[async_trait]
pub trait ObjectPageStream: Clone + Send {
    async fn next(&mut self) -> Result<Option<Vec<ObjectEntry>>>;
}

/// ObjectPageStreamer will convert an [`ObjectPageStream`] to [`ObjectStream`]
#[pin_project]
pub struct ObjectPageStreamer<S: ObjectPageStream> {
    inner: S,
    fut: Option<BoxFuture<'static, Result<Option<Vec<ObjectEntry>>>>>,
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
            fut: None,
            entries: vec![].into_iter(),
        }
    }
}

impl<S> Stream for ObjectPageStreamer<S>
where
    S: ObjectPageStream + 'static,
{
    type Item = Result<ObjectEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // Try to fetch entry from already cached entries.
            if let Some(entry) = this.entries.next() {
                return Poll::Ready(Some(Ok(entry)));
            }

            match &mut this.fut {
                None => {
                    let mut stream = this.inner.clone();
                    let fut = async move { stream.next().await };
                    *this.fut = Some(Box::pin(fut));
                }
                Some(fut) => match ready!(Pin::new(fut).poll(cx))? {
                    None => {
                        *this.fut = None;
                        return Poll::Ready(None);
                    }
                    Some(entries) => {
                        *this.entries = entries.into_iter();
                        *this.fut = None;
                    }
                },
            }
        }
    }
}
