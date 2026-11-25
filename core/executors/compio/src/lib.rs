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
#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::time::Duration;

use compio::dispatcher::Dispatcher;
use futures_timer::Delay;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::Execute;
use opendal_core::Result;
use opendal_core::raw::BoxedStaticFuture;

/// Runs OpenDAL background tasks on a Compio [`Dispatcher`].
///
/// Timers run independently of the dispatcher, including with a single worker
/// configured to run tasks sequentially.
///
/// Use [`Execute::try_execute`] to receive submission errors. The infallible
/// [`Execute::execute`] method logs submission errors.
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
        if let Err(err) = self.try_execute(f) {
            log::error!("{err}");
        }
    }

    fn try_execute(&self, f: BoxedStaticFuture<()>) -> Result<()> {
        let _completion = self
            .dispatcher
            .dispatch(move || f)
            .map_err(|_| Error::new(ErrorKind::Unexpected, "compio dispatcher is unavailable"))?;
        Ok(())
    }

    fn sleep(&self, duration: Duration) -> Option<BoxedStaticFuture<()>> {
        Some(Box::pin(Delay::new(duration)))
    }
}
