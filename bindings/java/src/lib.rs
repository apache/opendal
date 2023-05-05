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
use std::sync::Arc;

use jni::sys::jint;
use jni::sys::JNI_VERSION_1_8;
use jni::JavaVM;
use once_cell::sync::OnceCell;
use tokio::runtime::{Builder, Runtime};

mod macros;
mod operator;
mod util;

static mut RUNTIME: OnceCell<Runtime> = OnceCell::new();

thread_local! {
    static JAVA_VM: RefCell<Option<Arc<JavaVM>>> = RefCell::new(None);
    static JENV: RefCell<Option<*mut jni::sys::JNIEnv>> = RefCell::new(None);
}

/// # Safety
///
/// This function could be only called by java vm when load this lib.
#[no_mangle]
pub unsafe extern "system" fn JNI_OnLoad(vm: JavaVM, _: *mut c_void) -> jint {
    // TODO: make this configurable in the future
    let thread_count = num_cpus::get();

    let java_vm = Arc::new(vm);
    let runtime = Builder::new_multi_thread()
        .worker_threads(thread_count)
        .on_thread_start(move || {
            JENV.with(|cell| {
                let env = java_vm.attach_current_thread_as_daemon().unwrap();
                *cell.borrow_mut() = Some(env.get_raw());
            });
            JAVA_VM.with(|cell| {
                *cell.borrow_mut() = Some(java_vm.clone());
            });
        })
        .on_thread_stop(move || {
            JENV.with(|cell| {
                *cell.borrow_mut() = None;
            });
            JAVA_VM.with(|cell| unsafe {
                if let Some(vm) = cell.borrow_mut().take() {
                    vm.detach_current_thread();
                }
            });
        })
        .build()
        .unwrap();
    RUNTIME.set(runtime).unwrap();
    JNI_VERSION_1_8
}

/// # Safety
///
/// This function could be only called by java vm when unload this lib.
#[no_mangle]
pub unsafe extern "system" fn JNI_OnUnload(_: JavaVM, _: *mut c_void) {
    if let Some(runtime) = RUNTIME.take() {
        runtime.shutdown_background();
    }
}
