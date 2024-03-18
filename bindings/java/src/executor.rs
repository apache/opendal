use std::future::Future;

use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::task::JoinHandle;

use crate::Result;
use crate::{ENV, RUNTIME};

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

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_AsyncExecutor_makeTokioExecutor(
    mut env: JNIEnv,
    _: JClass,
    cores: usize,
) -> jlong {
    make_tokio_executor(&mut env, cores)
        .map(|executor| Box::into_raw(Box::new(executor)) as jlong)
        .unwrap_or_else(|e| {
            e.throw(&mut env);
            0
        })
}

pub(crate) fn make_tokio_executor(env: &mut JNIEnv, cores: usize) -> Result<Executor> {
    let vm = env.get_java_vm().expect("JavaVM must be available");
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores)
        .on_thread_start(move || {
            ENV.with(|cell| {
                let env = vm
                    .attach_current_thread_as_daemon()
                    .expect("attach thread must succeed");
                *cell.borrow_mut() = Some(env.get_raw());
            })
        })
        .enable_all()
        .build()
        .map_err(|e| {
            opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "Failed to create tokio runtime.",
            )
            .set_source(e)
        })?;
    Ok(Executor::Tokio(executor))
}

/// # Safety
///
/// This function could be only when the lib is loaded.
pub(crate) unsafe fn executor_or_default<'a>(
    env: &mut JNIEnv<'a>,
    executor: *const Executor,
) -> &'a Executor {
    if executor.is_null() {
        default_executor(env)
    } else {
        &*executor
    }
}

/// # Safety
///
/// This function could be only when the lib is loaded.
unsafe fn default_executor<'a>(env: &mut JNIEnv<'a>) -> &'a Executor {
    RUNTIME
        .get_or_try_init(|| make_tokio_executor(env, num_cpus::get()))
        .unwrap()
}
