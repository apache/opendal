use super::*;
use crate::raw::MaybeSend;
use crate::*;
use futures::FutureExt;
use std::future::Future;
use std::sync::Arc;

/// Executor that runs futures in background.
///
/// Executor is created by users and used by opendal. So it's by design that Executor only
/// expose constructor methods.
///
/// Executor will run futures in background and return a [`Task`] as handle to the future. Users
/// can call `task.await` to wait for the future to complete or drop the `Task` to cancel it.
pub struct Executor {
    executor: Arc<dyn Execute>,
}

#[cfg(feature = "executors-tokio")]
impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    /// Create a default executor.
    ///
    /// The default executor is enabled by feature flags.
    #[cfg(feature = "executors-tokio")]
    pub fn new() -> Self {
        Self::with(executors::TokioExecutor::default())
    }

    /// Create a new executor with given execute impl.
    pub fn with(exec: impl Execute) -> Self {
        Self {
            executor: Arc::new(exec),
        }
    }

    /// Run given future in background immediately.
    #[allow(unused)]
    pub(crate) fn execute<F>(&self, f: F) -> Result<Task<F::Output>>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend + 'static,
    {
        let (fut, handle) = f.remote_handle();
        self.executor.execute(Box::pin(fut))?;
        Ok(Task::new(handle))
    }
}
