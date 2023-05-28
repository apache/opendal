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
use std::collections::HashMap;
use std::ffi::c_void;

use jni::objects::JMap;
use jni::objects::JObject;
use jni::objects::JString;
use jni::sys::jint;
use jni::sys::JNI_VERSION_1_8;
use jni::JNIEnv;
use jni::JavaVM;
use once_cell::sync::OnceCell;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

mod blocking_operator;
mod error;
mod metadata;
mod operator;

pub(crate) type Result<T> = std::result::Result<T, error::Error>;

static mut RUNTIME: OnceCell<Runtime> = OnceCell::new();
thread_local! {
    static ENV: RefCell<Option<*mut jni::sys::JNIEnv>> = RefCell::new(None);
}

/// # Safety
///
/// This function could be only called by java vm when load this lib.
#[no_mangle]
pub unsafe extern "system" fn JNI_OnLoad(vm: JavaVM, _: *mut c_void) -> jint {
    RUNTIME
        .set(
            Builder::new_multi_thread()
                .worker_threads(num_cpus::get())
                .on_thread_start(move || {
                    ENV.with(|cell| {
                        let env = vm.attach_current_thread_as_daemon().unwrap();
                        *cell.borrow_mut() = Some(env.get_raw());
                    })
                })
                .build()
                .unwrap(),
        )
        .unwrap();

    JNI_VERSION_1_8
}

/// # Safety
///
/// This function could be only called by java vm when unload this lib.
#[no_mangle]
pub unsafe extern "system" fn JNI_OnUnload(_: JavaVM, _: *mut c_void) {
    if let Some(r) = RUNTIME.take() {
        r.shutdown_background()
    }
}

/// # Safety
///
/// This function could be only when the lib is loaded.
unsafe fn get_current_env<'local>() -> JNIEnv<'local> {
    let env = ENV.with(|cell| *cell.borrow_mut()).unwrap();
    JNIEnv::from_raw(env).unwrap()
}

/// # Safety
///
/// This function could be only when the lib is loaded.
unsafe fn get_global_runtime<'local>() -> &'local Runtime {
    RUNTIME.get_unchecked()
}

fn jmap_to_hashmap(env: &mut JNIEnv, params: &JObject) -> Result<HashMap<String, String>> {
    let map = JMap::from_env(env, params)?;
    let mut iter = map.iter(env)?;

    let mut result: HashMap<String, String> = HashMap::new();
    while let Some(e) = iter.next(env)? {
        let k = JString::from(e.0);
        let v = JString::from(e.1);
        result.insert(env.get_string(&k)?.into(), env.get_string(&v)?.into());
    }

    Ok(result)
}
