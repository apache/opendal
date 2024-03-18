use crate::ENV;
use jni::JavaVM;
use std::future::Future;
use std::io;
use tokio::task::JoinHandle;

pub enum Executor {
    Tokio(tokio::runtime::Runtime),
}

impl Executor {
    pub fn shutdown_background(self) {
        match self {
            Executor::Tokio(e) => e.shutdown_background(),
        }
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self {
            Executor::Tokio(e) => e.spawn(future),
        }
    }

    pub fn enter_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        match self {
            Executor::Tokio(e) => {
                let _guard = e.enter();
                f()
            }
        }
    }
}

pub(crate) fn make_tokio_executor(cores: usize, vm: JavaVM) -> io::Result<Executor> {
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores)
        .on_thread_start(move || {
            ENV.with(|cell| {
                let env = vm.attach_current_thread_as_daemon().unwrap();
                *cell.borrow_mut() = Some(env.get_raw());
            })
        })
        .enable_all()
        .build()?;
    Ok(Executor::Tokio(executor))
}
