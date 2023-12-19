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
use futures::TryStreamExt;

use crate::raw::*;
use crate::*;

/// Convert given futures stream into [`oio::Stream`].
pub fn into_stream<S>(stream: S) -> IntoStream<S>
where
    S: futures::Stream<Item = Result<Bytes>> + Send + Sync + Unpin,
{
    IntoStream { inner: stream }
}

pub struct IntoStream<S> {
    inner: S,
}

impl<S> oio::Stream for IntoStream<S>
where
    S: futures::Stream<Item = Result<Bytes>> + Send + Sync + Unpin,
{
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        self.inner.try_poll_next_unpin(cx)
    }
}
