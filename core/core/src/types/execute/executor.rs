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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use super::*;
use crate::Result;
use crate::raw::BoxedStaticFuture;
use crate::raw::MaybeSend;

static DEFAULT_EXECUTOR: OnceLock<Executor> = OnceLock::new();

/// Executor that runs futures in background.
///
/// Executor is created by users and used by opendal. So it's by design that Executor only
/// expose constructor methods.
///
/// Executor will run futures in background and return a `Task` as handle to the future. Users
/// can call `task.await` to wait for the future to complete or drop the `Task` to cancel it.
#[derive(Clone)]
pub struct Executor {
    executor: Arc<dyn Execute>,
}

impl Debug for Executor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Executor")
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    /// Create an executor that uses the process-wide default for each task.
    ///
    /// Concurrent I/O returns an error if no default has been installed.
    pub fn new() -> Self {
        Self::with(DefaultExecutor)
    }

    /// Create a new executor with given execute impl.
    pub fn with(exec: impl Execute) -> Self {
        Self {
            executor: Arc::new(exec),
        }
    }

    /// Install the process-wide default executor.
    ///
    /// The first installed executor wins. Later calls are ignored.
    /// The `opendal` facade installs Tokio when its `executors-tokio` and
    /// `auto-register-services` features are both enabled.
    pub fn install_default(exec: impl Execute) {
        let _ = DEFAULT_EXECUTOR.set(Self::with(exec));
    }

    /// Return the inner executor.
    pub fn into_inner(self) -> Arc<dyn Execute> {
        self.executor
    }

    /// Return a reference to the inner executor.
    pub fn inner(&self) -> &dyn Execute {
        self.executor.as_ref()
    }

    /// Return the configured timeout for an individual I/O task.
    pub(crate) fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        self.executor.timeout()
    }

    /// Run given future in background immediately.
    pub(crate) fn execute<F>(&self, f: F) -> Result<Task<F::Output>>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend + 'static,
    {
        Task::submit(self.inner(), f)
    }
}

struct DefaultExecutor;

impl Execute for DefaultExecutor {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        match DEFAULT_EXECUTOR.get() {
            Some(executor) => executor.inner().execute(f),
            None => panic!("default executor is not installed"),
        }
    }

    fn try_execute(&self, f: BoxedStaticFuture<()>) -> Result<()> {
        match DEFAULT_EXECUTOR.get() {
            Some(executor) => executor.inner().try_execute(f),
            None => ().try_execute(f),
        }
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        DEFAULT_EXECUTOR.get()?.timeout()
    }

    fn sleep(&self, duration: Duration) -> Option<BoxedStaticFuture<()>> {
        DEFAULT_EXECUTOR.get()?.inner().sleep(duration)
    }
}
