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
                Ok(ds) => {
                    self.state = WalkTopDownState::Listing(ds);
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
    dirs: Vec<Object>,
    ds: Vec<DirStreamer>,
    state: WalkBottomUpState,
}

impl WalkBottomUp {
    pub fn new(parent: Object) -> Self {
        WalkBottomUp {
            dirs: Vec::new(),
            ds: Vec::new(),
            state: WalkBottomUpState::Starting(Some(parent)),
        }
    }
}

enum WalkBottomUpState {
    Starting(Option<Object>),
    Sending(BoxFuture<'static, Result<DirStreamer>>),
    Listing,
}

impl futures::Stream for WalkBottomUp {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            WalkBottomUpState::Starting(o) => {
                let o = o.take().expect("object must be valid");

                self.dirs.push(o.clone());

                let future = async move { o.list().await };

                self.state = WalkBottomUpState::Sending(Box::pin(future));
                self.poll_next(cx)
            }
            WalkBottomUpState::Sending(fut) => match ready!(Pin::new(fut).poll(cx)) {
                Ok(ds) => {
                    self.ds.push(ds);
                    self.state = WalkBottomUpState::Listing;
                    self.poll_next(cx)
                }
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            WalkBottomUpState::Listing => match self.ds.last_mut() {
                Some(ds) => match ready!(Pin::new(ds).poll_next(cx)) {
                    Some(Ok(de)) => {
                        if de.mode().is_dir() {
                            self.state = WalkBottomUpState::Starting(Some(de.into()));
                            self.poll_next(cx)
                        } else {
                            Poll::Ready(Some(Ok(de)))
                        }
                    }
                    Some(Err(e)) => Poll::Ready(Some(Err(e))),
                    None => {
                        let _ = self.ds.pop();
                        let dob = self
                            .dirs
                            .pop()
                            .expect("dis streamer corresponding object must exist");
                        let de = DirEntry::new(dob.accessor(), ObjectMode::DIR, dob.path());
                        Poll::Ready(Some(Ok(de)))
                    }
                },
                None => Poll::Ready(None),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Accessor;
    use futures::TryStreamExt;
    use log::debug;

    use crate::services::memory::Backend;

    fn get_position(vs: &Vec<String>, s: &str) -> usize {
        vs.iter()
            .position(|v| v == s)
            .expect("{s} is not found in {vs}")
    }

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

        let w = WalkTopDown::new(op.object("x/"));
        let actual = w
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|v| v.path().to_string())
            .collect::<Vec<_>>();

        debug!("walk top down: {:?}", actual);

        assert!(get_position(&actual, "x/x/x/x/") > get_position(&actual, "x/x/x/"));
        assert!(get_position(&actual, "x/x/x/") > get_position(&actual, "x/x/"));
        assert!(get_position(&actual, "x/x/") > get_position(&actual, "x/"));
        Ok(())
    }

    #[tokio::test]
    async fn test_walk_bottom_up() -> Result<()> {
        let _ = env_logger::try_init();

        let op = Operator::new(Backend::build().finish().await?);
        op.object("x/").create().await?;
        op.object("x/y").create().await?;
        op.object("x/x/").create().await?;
        op.object("x/x/y").create().await?;
        op.object("x/x/x/").create().await?;
        op.object("x/x/x/y").create().await?;
        op.object("x/x/x/x/").create().await?;

        let w = WalkBottomUp::new(op.object("x/"));
        let actual = w
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|v| v.path().to_string())
            .collect::<Vec<_>>();

        debug!("walk bottom up: {:?}", actual);

        assert!(get_position(&actual, "x/x/x/x/") < get_position(&actual, "x/x/x/"));
        assert!(get_position(&actual, "x/x/x/") < get_position(&actual, "x/x/"));
        assert!(get_position(&actual, "x/x/") < get_position(&actual, "x/"));
        Ok(())
    }
}
