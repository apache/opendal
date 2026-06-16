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

use std::ffi::c_void;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::available_parallelism;

use jni::Env;
use jni::EnvUnowned;
use jni::JavaVM;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JValue;
use jni::sys::{jint, jlong};
use tokio::task::JoinHandle;

use crate::Result;
use crate::error::ThrowOpenDal;

static mut RUNTIME: OnceLock<Executor> = OnceLock::new();

#[unsafe(no_mangle)]
pub unsafe extern "system" fn JNI_OnLoad(vm: *mut jni::sys::JavaVM, _: *mut c_void) -> jint {
    // Register the JavaVM singleton so worker threads can attach to the JVM
    // later via `JavaVM::singleton()`.
    let _ = unsafe { JavaVM::from_raw(vm) };
    opendal::init_default_registry();
    jni::sys::JNI_VERSION_1_8
}

/// # Safety
///
/// This function could be only called by java vm when unload this lib.
#[allow(static_mut_refs)]
#[unsafe(no_mangle)]
pub unsafe extern "system" fn JNI_OnUnload(_: *mut jni::sys::JavaVM, _: *mut c_void) {
    unsafe {
        RUNTIME.take();
    }
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

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_AsyncExecutor_makeTokioExecutor<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    cores: usize,
) -> jlong {
    env.with_env(|_env| -> Result<jlong> {
        let executor = make_tokio_executor(cores)?;
        Ok(Box::into_raw(Box::new(executor)) as jlong)
    })
    .resolve::<ThrowOpenDal>()
}

/// # Safety
///
/// This function should not be called before the AsyncExecutor is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncExecutor_disposeInternal<'local>(
    _: EnvUnowned<'local>,
    _: JObject<'local>,
    executor: *mut Executor,
) {
    unsafe {
        drop(Box::from_raw(executor));
    }
}

pub(crate) fn make_tokio_executor(cores: usize) -> Result<Executor> {
    let counter = AtomicUsize::new(0);
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores)
        .thread_name_fn(move || {
            let id = counter.fetch_add(1, Ordering::SeqCst);
            format!("opendal-tokio-worker-{id}")
        })
        .on_thread_start(|| {
            // `attach_current_thread` creates a permanent attachment; the thread
            // is detached automatically when it exits.
            let vm = JavaVM::singleton().expect("JavaVM singleton must be initialized");
            vm.attach_current_thread(set_current_thread_name)
                .expect("attach current thread must succeed");
        })
        .on_thread_stop(|| {
            // Typically, the thread attached to the JVM will be detached automatically
            // when the thread exits. However, there are some edge cases on Windows that
            // may lead to deadlocks. To mitigate this, we explicitly detach the thread here.
            //
            // See https://github.com/apache/opendal/issues/6869 and
            // https://github.com/jni-rs/jni-rs/issues/701 for more details.
            if let Ok(vm) = JavaVM::singleton() {
                let _ = vm.detach_current_thread();
            }
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

fn set_current_thread_name(env: &mut Env) -> Result<()> {
    let current_thread = env
        .call_static_method(
            jni_str!("java/lang/Thread"),
            jni_str!("currentThread"),
            jni_sig!("()Ljava/lang/Thread;"),
            &[],
        )?
        .l()?;
    let thread_name = match std::thread::current().name() {
        Some(thread_name) => env.new_string(thread_name)?,
        None => unreachable!("thread name must be set"),
    };
    env.call_method(
        &current_thread,
        jni_str!("setName"),
        jni_sig!("(Ljava/lang/String;)V"),
        &[JValue::Object(&thread_name)],
    )?;
    Ok(())
}

/// # Panic
///
/// Crash if the executor is disposed.
#[inline]
pub(crate) fn executor_or_default(executor: *const Executor) -> Result<&'static Executor> {
    unsafe {
        if executor.is_null() {
            default_executor()
        } else {
            // SAFETY: executor must be valid
            Ok(&*executor)
        }
    }
}

/// # Safety
///
/// This function could be only when the lib is loaded.
#[allow(static_mut_refs)]
unsafe fn default_executor() -> Result<&'static Executor> {
    // Return the executor if it's already initialized
    if let Some(runtime) = unsafe { RUNTIME.get() } {
        return Ok(runtime);
    }

    // Try to initialize the executor
    let executor =
        make_tokio_executor(available_parallelism().map(NonZeroUsize::get).unwrap_or(1))?;

    Ok(unsafe { RUNTIME.get_or_init(|| executor) })
}
