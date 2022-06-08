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
use std::collections::VecDeque;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{BytesStream, DirEntry, DirStreamer, Object, ObjectMode, Operator};

pub struct WalkTopDown {
    dirs: VecDeque<Object>,
    state: WalkTopDownState,
}

impl WalkTopDown {
    pub fn new(parent: Object) -> Self {
        WalkTopDown {
            dirs: VecDeque::from([parent]),
            state: WalkTopDownState::Idle,
        }
    }
}

enum WalkTopDownState {
    Idle,
    Sending(BoxFuture<'static, Result<DirStreamer>>),
    Listing(DirStreamer),
}

impl futures::Stream for WalkTopDown {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            WalkTopDownState::Idle => {
                let object = match self.dirs.pop_front() {
                    Some(o) => o,
                    None => return Poll::Ready(None),
                };
                let de = DirEntry::new(object.accessor(), ObjectMode::DIR, object.path());
                let future = async move { object.list().await };

                self.state = WalkTopDownState::Sending(Box::pin(future));
                return Poll::Ready(Some(Ok(de)));
            }
            WalkTopDownState::Sending(fut) => match ready!(Pin::new(fut).poll(cx)) {
                Ok(obs) => {
                    self.state = WalkTopDownState::Listing(obs);
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            WalkTopDownState::Listing(ds) => match ready!(Pin::new(ds).poll_next(cx)) {
                Some(Ok(de)) => {
                    if de.mode().is_dir() {
                        self.dirs.push_back(de.into());
                        self.poll_next(cx)
                    } else {
                        Poll::Ready(Some(Ok(de)))
                    }
                }
                Some(Err(e)) => Poll::Ready(Some(Err(e))),
                None => {
                    self.state = WalkTopDownState::Idle;
                    self.poll_next(cx)
                }
            },
        }
    }
}

pub struct WalkBottomUp {
    op: Operator,
    path: String,
}

impl futures::Stream for WalkBottomUp {
    type Item = Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Accessor;
    use futures::TryStreamExt;

    use crate::services::memory::Backend;

    #[tokio::test]
    async fn test_walk_top_down() -> Result<()> {
        let _ = env_logger::try_init();

        let op = Operator::new(Backend::build().finish().await?);
        op.object("x/").create().await?;
        op.object("x/y").create().await?;
        op.object("x/x/").create().await?;
        op.object("x/x/y").create().await?;
        op.object("x/x/x/").create().await?;
        op.object("x/x/x/y").create().await?;
        op.object("x/x/x/x/").create().await?;

        let expect = vec![
            "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
        ]
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>();

        let w = WalkTopDown::new(op.object("x/"));
        let actual = w
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|v| v.path().to_string())
            .collect::<Vec<_>>();

        assert_eq!(actual, expect);
        Ok(())
    }
}
