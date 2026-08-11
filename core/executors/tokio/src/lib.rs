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

use std::time::Duration;

use opendal_core::Execute;
use opendal_core::Executor;
use opendal_core::raw::BoxedStaticFuture;

/// Executor that uses [`tokio::task::spawn`] to execute futures.
#[derive(Default)]
pub struct TokioExecutor {}

impl Execute for TokioExecutor {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        let _handle = tokio::task::spawn(f);
    }

    fn timeout(&self, timeout: Duration) -> BoxedStaticFuture<()> {
        Box::pin(tokio::time::sleep(timeout))
    }
}

/// Install Tokio as the process-wide default OpenDAL executor.
#[doc(hidden)]
pub fn install_default() {
    Executor::install_default(TokioExecutor::default());
}

#[cfg(test)]
mod tests {
    use futures::channel::oneshot;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_default_tokio_executor() {
        let executor = Executor::default();
        install_default();
        let (tx, rx) = oneshot::channel();

        executor.inner().execute(Box::pin(async move {
            let _ = tx.send(());
        }));

        timeout(Duration::from_secs(1), rx)
            .await
            .expect("task should finish before timeout")
            .expect("task should send completion");
    }
}
