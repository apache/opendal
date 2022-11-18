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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::Stream;

use crate::Accessor;
use crate::ObjectEntry;
use crate::ObjectIterator;
use crate::ObjectStreamer;
use crate::Result;

/// set_accessor_for_object_steamer will fix the accessor for object entry.
pub fn set_accessor_for_object_steamer(
    inner: ObjectStreamer,
    acc: impl Accessor,
) -> ObjectStreamer {
    Box::new(AccessorSetterObjectStreamer::new(Arc::new(acc), inner)) as ObjectStreamer
}

/// AccessorSetterObjectStreamer will set the inner accessor for object entry.
struct AccessorSetterObjectStreamer {
    acc: Arc<dyn Accessor>,
    inner: ObjectStreamer,
}

impl AccessorSetterObjectStreamer {
    /// Create a new AccessorSetterObjectStreamer.
    fn new(acc: Arc<dyn Accessor>, inner: ObjectStreamer) -> Self {
        Self { acc, inner }
    }
}

impl Stream for AccessorSetterObjectStreamer {
    type Item = Result<ObjectEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(mut de))) => {
                de.set_accessor(self.acc.clone());
                Poll::Ready(Some(Ok(de)))
            }
            v => v,
        }
    }
}

/// set_accessor_for_object_iterator will fix the accessor for object entry.
pub fn set_accessor_for_object_iterator(
    inner: ObjectIterator,
    acc: impl Accessor,
) -> ObjectIterator {
    Box::new(AccessorSetterObjectIterator::new(Arc::new(acc), inner)) as ObjectIterator
}

/// AccessorSetterObjectIterator that set accessor for entry.
struct AccessorSetterObjectIterator {
    acc: Arc<dyn Accessor>,
    inner: ObjectIterator,
}

impl AccessorSetterObjectIterator {
    /// Create a new AccessorSetterObjectIterator.
    fn new(acc: Arc<dyn Accessor>, inner: ObjectIterator) -> Self {
        Self { acc, inner }
    }
}

impl Iterator for AccessorSetterObjectIterator {
    type Item = Result<ObjectEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(mut de)) => {
                de.set_accessor(self.acc.clone());
                Some(Ok(de))
            }
            v => v,
        }
    }
}
