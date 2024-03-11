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

use std::collections::VecDeque;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;

use crate::raw::*;
use crate::*;

/// PageList is used to implement [`List`] based on API supporting pagination. By implementing
/// PageList, services don't need to care about the details of page list.
///
/// # Architecture
///
/// The architecture after adopting [`PageList`]:
///
/// - Services impl `PageList`
/// - `PageLister` impl `List`
/// - Expose `PageLister` as `Accessor::Lister`
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PageList: Send + Sync + Unpin + 'static {
    /// next_page is used to fetch next page of entries from underlying storage.
    async fn next_page(&self, ctx: &mut PageContext) -> Result<()>;
}

/// PageContext is the context passing between `PageList`.
///
/// [`PageLister`] will init the PageContext, and implementer of [`PageList`] should fill the `PageContext`
/// based on their needs.
///
/// - Set `done` to `true` if all page have been fetched.
/// - Update `token` if there is more page to fetch. `token` is not exposed to users, it's internal used only.
/// - Push back into the entries for each entry fetched from underlying storage.
///
/// NOTE: `entries` is a `VecDeque` to avoid unnecessary memory allocation. Only `push_back` is allowed.
pub struct PageContext {
    /// done is used to indicate whether the list operation is done.
    pub done: bool,
    /// token is used by underlying storage services to fetch next page.
    pub token: String,
    /// entries is used to store entries fetched from underlying storage.
    ///
    /// Please always reuse the same `VecDeque` to avoid unnecessary memory allocation.
    /// PageLister makes sure that entries is reset before calling `next_page`. Implementer
    /// can calling `push_back` on `entries` directly.
    pub entries: VecDeque<oio::Entry>,
}

/// PageLister implements [`List`] based on [`PageList`].
pub struct PageLister<L: PageList> {
    state: State<L>,
}

enum State<L> {
    Idle(Option<(L, PageContext)>),
    Fetch(BoxedFuture<((L, PageContext), Result<()>)>),
}

/// # Safety
///
/// wasm32 is a special target that we only have one event-loop for this state.
unsafe impl<L: PageList> Send for State<L> {}
/// # Safety
///
/// We will only take `&mut Self` reference for State.
unsafe impl<L: PageList> Sync for State<L> {}

impl<L> PageLister<L>
where
    L: PageList,
{
    /// Create a new PageLister.
    pub fn new(l: L) -> Self {
        Self {
            state: State::Idle(Some((
                l,
                PageContext {
                    done: false,
                    token: "".to_string(),
                    entries: VecDeque::new(),
                },
            ))),
        }
    }
}

impl<L> oio::List for PageLister<L>
where
    L: PageList,
{
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        loop {
            match &mut self.state {
                State::Idle(st) => {
                    if let Some((_, ctx)) = st.as_mut() {
                        if let Some(entry) = ctx.entries.pop_front() {
                            return Poll::Ready(Ok(Some(entry)));
                        }
                        if ctx.done {
                            return Poll::Ready(Ok(None));
                        }
                    }

                    let (l, mut ctx) = st.take().expect("lister must be valid");
                    let fut = async move {
                        let res = l.next_page(&mut ctx).await;
                        ((l, ctx), res)
                    };
                    self.state = State::Fetch(Box::pin(fut));
                }
                State::Fetch(fut) => {
                    let ((l, ctx), res) = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle(Some((l, ctx)));

                    res?;
                }
            }
        }
    }
}
