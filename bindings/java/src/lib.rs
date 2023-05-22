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
use std::str::FromStr;

use jni::objects::JMap;
use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::JThrowable;
use jni::objects::JValue;
use jni::sys::jint;
use jni::sys::JNI_VERSION_1_8;
use jni::{JNIEnv, JavaVM};
use once_cell::sync::OnceCell;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

use opendal::ErrorKind;

mod blocking_operator;
mod metadata;
mod operator;

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

fn convert_error_to_exception<'local>(
    env: &mut JNIEnv<'local>,
    error: opendal::Error,
) -> Result<JThrowable<'local>, jni::errors::Error> {
    let class = env.find_class("org/apache/opendal/exception/ODException")?;

    let code = env.new_string(match error.kind() {
        ErrorKind::Unexpected => "Unexpected",
        ErrorKind::Unsupported => "Unsupported",
        ErrorKind::ConfigInvalid => "ConfigInvalid",
        ErrorKind::NotFound => "NotFound",
        ErrorKind::PermissionDenied => "PermissionDenied",
        ErrorKind::IsADirectory => "IsADirectory",
        ErrorKind::NotADirectory => "NotADirectory",
        ErrorKind::AlreadyExists => "AlreadyExists",
        ErrorKind::RateLimited => "RateLimited",
        ErrorKind::IsSameFile => "IsSameFile",
        ErrorKind::ConditionNotMatch => "ConditionNotMatch",
        ErrorKind::ContentTruncated => "ContentTruncated",
        ErrorKind::ContentIncomplete => "ContentIncomplete",
        _ => "Unexpected",
    })?;
    let message = env.new_string(error.to_string())?;

    let sig = "(Ljava/lang/String;Ljava/lang/String;)V";
    let params = &[JValue::Object(&code), JValue::Object(&message)];
    env.new_object(class, sig, params).map(JThrowable::from)
}

fn convert_jmap_to_hashmap(env: &mut JNIEnv, params: &JObject) -> HashMap<String, String> {
    let map = JMap::from_env(env, params).unwrap();
    let mut iter = map.iter(env).unwrap();

    let mut result: HashMap<String, String> = HashMap::new();
    while let Some(e) = iter.next(env).unwrap() {
        let k = JString::from(e.0);
        let v = JString::from(e.1);
        result.insert(
            env.get_string(&k).unwrap().into(),
            env.get_string(&v).unwrap().into(),
        );
    }
    result
}
