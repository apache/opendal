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

use futures::future::BoxFuture;
use futures::Future;
use futures::{ready, StreamExt};
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{BytesStream, Object, ObjectStreamer, Operator};

pub struct WalkTopDown {
    parent: Object,
    dirs: Vec<Object>,
    state: WalkTopDownState,
}

enum WalkTopDownState {
    Idle,
    Sending(BoxFuture<'static, Result<ObjectStreamer>>),
    Listing(ObjectStreamer),
}

impl futures::Stream for WalkTopDown {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            WalkTopDownState::Idle => {
                let object = self.parent.clone();
                let future = async move { object.list().await };

                self.state = WalkTopDownState::Sending(Box::pin(future));
                self.poll_next(cx)
            }
            WalkTopDownState::Sending(fut) => match ready!(Pin::new(fut).poll(cx)) {
                Ok(obs) => {
                    self.state = WalkTopDownState::Listing(obs);
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            WalkTopDownState::Listing(obs) => match ready!(Pin::new(obs).poll_next(cx)) {
                Some(Ok(mut o)) => {}
                v => Poll::Ready(v),
            },
        }
    }
}

pub struct WalkBottomUp {
    op: Operator,
    path: String,
}

impl futures::Stream for WalkBottomUp {
    type Item = Result<Object>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
