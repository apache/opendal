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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use compio::dispatcher::Dispatcher;
use futures::channel::oneshot;
use opendal_core::Execute;
use opendal_core::raw::BoxedStaticFuture;

/// Executor that runs futures and timers on a Compio [`Dispatcher`].
///
/// Task submission and timer creation panic if the dispatcher cannot accept
/// work, such as after all worker threads stop.
#[derive(Clone)]
pub struct CompioExecutor {
    dispatcher: Arc<Dispatcher>,
}

impl CompioExecutor {
    /// Create an executor backed by the given dispatcher.
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self { dispatcher }
    }
}

impl Execute for CompioExecutor {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        let _ = self.dispatcher.dispatch(move || f);
    }

    fn timeout(&self, timeout: Duration) -> BoxedStaticFuture<()> {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let receiver = match self
            .dispatcher
            .dispatch(move || async move { compio::time::timeout(timeout, cancel_rx).await })
        {
            Ok(receiver) => receiver,
            Err(_) => panic!("compio dispatcher is unavailable"),
        };
        Box::pin(async move {
            let _cancel_on_drop = cancel_tx;
            let _ = receiver.await;
        })
    }
}
