// Copyright 2021 Sean McArthur <sean@seanmonstar.com>.
//
// This mod is mainly borrowed from reqwest (https://github.com/seanmonstar/reqwest)
//
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

use std::future::Future;
use std::io::Result;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread::{self, Thread};
use std::time::Duration;
use tokio::sync::oneshot;

use tokio::time::Instant;

pub(crate) fn timeout<F, I>(fut: F, timeout: Option<Duration>) -> Result<I>
where
    F: Future<Output = Result<I>>,
{
    enter();

    let deadline = timeout.map(|d| {
        log::trace!("wait at most {:?}", d);
        Instant::now() + d
    });

    let thread = ThreadWaker(thread::current());
    // Arc shouldn't be necessary, since `Thread` is reference counted internally,
    // but let's just stay safe for now.
    let waker = futures::task::waker(Arc::new(thread));
    let mut cx = Context::from_waker(&waker);

    futures::pin_mut!(fut);

    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(val)) => return Ok(val),
            Poll::Ready(Err(err)) => return Err(err),
            Poll::Pending => (), // fallthrough
        }

        if let Some(deadline) = deadline {
            let now = Instant::now();
            if now >= deadline {
                log::trace!("wait timeout exceeded");
                return Err(Error::new(ErrorKind::TimedOut, "wait timeout exceeded"));
            }

            log::trace!(
                "({:?}) park timeout {:?}",
                thread::current().id(),
                deadline - now
            );
            thread::park_timeout(deadline - now);
        } else {
            log::trace!("({:?}) park without timeout", thread::current().id());
            thread::park();
        }
    }
}

struct ThreadWaker(Thread);

impl futures::task::ArcWake for ThreadWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.unpark();
    }
}

fn enter() {
    // Check we aren't already in a runtime
    #[cfg(debug_assertions)]
    {
        let _enter = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build shell runtime")
            .enter();
    }
}

pub async fn forward<F>(
    fut: F,
    mut tx: oneshot::Sender<hyper::Result<hyper::Response<hyper::Body>>>,
) where
    F: Future<Output = hyper::Result<hyper::Response<hyper::Body>>>,
{
    futures::pin_mut!(fut);

    // "select" on the sender being canceled, and the future completing
    let res = futures::future::poll_fn(|cx| {
        match fut.as_mut().poll(cx) {
            Poll::Ready(val) => Poll::Ready(Some(val)),
            Poll::Pending => {
                // check if the callback is canceled
                futures::ready!(tx.poll_closed(cx));
                Poll::Ready(None)
            }
        }
    })
    .await;

    if let Some(res) = res {
        let _ = tx.send(res);
    }
    // else request is canceled
}
