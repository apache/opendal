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

use std::cell::RefCell;
use std::ffi::c_void;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::thread::available_parallelism;

use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::sys::jlong;
use jni::JNIEnv;
use jni::JavaVM;
use tokio::task::JoinHandle;

use crate::Result;

static RUNTIME: OnceLock<Executor> = OnceLock::new();
thread_local! {
    static ENV: RefCell<Option<*mut jni::sys::JNIEnv>> = const { RefCell::new(None) };
}

/// # Safety
///
/// This function could be only called by java vm when unload this lib.
#[no_mangle]
pub unsafe extern "system" fn JNI_OnUnload(_: JavaVM, _: *mut c_void) {
    // Since OnceLock doesn't have a take() method, we can't remove the runtime
    // This is fine as the JVM is shutting down anyway
    // The runtime will be dropped when the process exits
}

/// # Safety
///
/// This function could be only called when the lib is loaded and within an executor thread.
pub(crate) unsafe fn get_current_env<'local>() -> JNIEnv<'local> {
    let env = ENV
        .with(|cell| *cell.borrow_mut())
        .expect("env must be available");
    JNIEnv::from_raw(env).expect("env must be valid")
}

pub enum Executor {
    Tokio(tokio::runtime::Runtime),
}

impl Executor {
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

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self {
            Executor::Tokio(e) => e.spawn(future),
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

/// # Safety
///
/// This function should not be called before the AsyncExecutor is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncExecutor_disposeInternal(
    _: JNIEnv,
    _: JObject,
    executor: *mut Executor,
) {
    drop(Box::from_raw(executor));
}

pub(crate) fn make_tokio_executor(env: &mut JNIEnv, cores: usize) -> Result<Executor> {
    let vm = env.get_java_vm().expect("JavaVM must be available");
    let counter = AtomicUsize::new(0);
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores)
        .thread_name_fn(move || {
            let id = counter.fetch_add(1, Ordering::SeqCst);
            format!("opendal-tokio-worker-{}", id)
        })
        .on_thread_start(move || {
            ENV.with(|cell| {
                let mut env = vm
                    .attach_current_thread_as_daemon()
                    .expect("attach thread must succeed");

                set_current_thread_name(&mut env).expect("current thread name has been set above");

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

fn set_current_thread_name(env: &mut JNIEnv) -> Result<()> {
    let current_thread = env
        .call_static_method(
            "java/lang/Thread",
            "currentThread",
            "()Ljava/lang/Thread;",
            &[],
        )?
        .l()?;
    let thread_name = match std::thread::current().name() {
        Some(thread_name) => env.new_string(thread_name)?,
        None => unreachable!("thread name must be set"),
    };
    env.call_method(
        current_thread,
        "setName",
        "(Ljava/lang/String;)V",
        &[JValue::Object(&thread_name)],
    )?;
    Ok(())
}

/// # Panic
///
/// Crash if the executor is disposed.
#[inline]
pub(crate) fn executor_or_default<'a>(
    env: &mut JNIEnv<'a>,
    executor: *const Executor,
) -> Result<&'a Executor> {
    unsafe {
        if executor.is_null() {
            default_executor(env)
        } else {
            // SAFETY: executor must be valid
            Ok(&*executor)
        }
    }
}

/// # Safety
///
/// This function could be only when the lib is loaded.
unsafe fn default_executor<'a>(env: &mut JNIEnv<'a>) -> Result<&'a Executor> {
    Ok(RUNTIME.get_or_init(|| {
        make_tokio_executor(
            env,
            available_parallelism().map(NonZeroUsize::get).unwrap_or(1),
        )
        .expect("Failed to initialize default executor")
    }))
}
