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

use std::task::Context;
use std::task::Poll;

use bytes::Bytes;

use crate::*;

/// Streamer is a type erased [`Stream`].
pub type Streamer = Box<dyn Stream>;

/// Stream is the trait that OpenDAL accepts for sinking data.
///
/// It's nearly the same with [`futures::Stream`], but it satisfied
/// `Unpin` + `Send` + `Sync`. And the item is `Result<Bytes>`.
pub trait Stream: Unpin + Send + Sync {
    /// Poll next item `Result<Bytes>` from the stream.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>>;
}

impl Stream for () {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        unimplemented!("poll_next is required to be implemented for oio::Stream")
    }
}

/// `Box<dyn Stream>` won't implement `Stream` automatically.
/// To make Streamer work as expected, we must add this impl.
impl<T: Stream + ?Sized> Stream for Box<T> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        (**self).poll_next(cx)
    }
}

impl futures::Stream for dyn Stream {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this: &mut dyn Stream = &mut *self;

        this.poll_next(cx)
    }
}
