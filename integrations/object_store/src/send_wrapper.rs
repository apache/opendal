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

//! Conditionally add the `Send` marker trait for the wrapped type.
//! Only take effect when the `send_wrapper` feature is enabled.

use futures::Stream;
#[cfg(feature = "send_wrapper")]
pub use send_wrapper::SendWrapper;

#[cfg(not(feature = "send_wrapper"))]
pub use noop_wrapper::NoopWrapper as SendWrapper;

#[cfg(not(feature = "send_wrapper"))]
mod noop_wrapper {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{Future, Stream};
    use pin_project::pin_project;

    #[pin_project]
    pub struct NoopWrapper<T> {
        #[pin]
        item: T,
    }

    impl<T> Future for NoopWrapper<T>
    where
        T: Future,
    {
        type Output = T::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.project();
            this.item.poll(cx)
        }
    }

    impl<T> Stream for NoopWrapper<T>
    where
        T: Stream,
    {
        type Item = T::Item;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.project();
            this.item.poll_next(cx)
        }
    }

    impl<T> NoopWrapper<T> {
        pub fn new(item: T) -> Self {
            Self { item }
        }
    }
}

pub trait IntoSendFuture {
    type Output;

    fn into_send(self) -> Self::Output;
}

impl<T> IntoSendFuture for T
where
    T: futures::Future,
{
    type Output = SendWrapper<T>;
    fn into_send(self) -> Self::Output {
        SendWrapper::new(self)
    }
}

pub trait IntoSendStream {
    type Output;

    fn into_send(self) -> Self::Output;
}

impl<T> IntoSendStream for T
where
    T: Stream,
{
    type Output = SendWrapper<T>;
    fn into_send(self) -> Self::Output {
        SendWrapper::new(self)
    }
}
