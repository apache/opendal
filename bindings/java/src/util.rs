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

use std::collections::HashMap;

use jni::objects::{JMap, JObject, JString, JThrowable, JValue};
use jni::JNIEnv;
use tokio::runtime::Runtime;

use crate::{JENV, RUNTIME};

pub(crate) unsafe fn get_runtime() -> &'static Runtime {
    RUNTIME.get().unwrap()
}

pub(crate) unsafe fn jobject_into_map(
    env: &mut JNIEnv,
    params: &JObject,
) -> HashMap<String, String> {
    let mut result: HashMap<String, String> = HashMap::new();
    let _ = JMap::from_env(env, params)
        .unwrap()
        .iter(env)
        .and_then(|mut iter| {
            while let Some(e) = iter.next(env)? {
                let key = JString::from(e.0);
                let value = JString::from(e.1);
                let key = jstring_into_string(env, &key);
                let value = jstring_into_string(env, &value);
                result.insert(key, value);
            }
            Ok(())
        });
    result
}

/// # Safety
///
/// This function should be called only when input is a none null java string.
pub(crate) unsafe fn jstring_into_string(env: &JNIEnv, input: &JString) -> String {
    env.get_string_unchecked(input).unwrap().into()
}

pub(crate) fn opendal_error_into_exception<'local>(
    env: &mut JNIEnv<'local>,
    error: opendal::Error,
) -> Result<JThrowable<'local>, jni::errors::Error> {
    new_exception(env, error.kind().into_static(), &error.to_string())
}

pub(crate) fn vec_u8_into_i8(v: Vec<u8>) -> Vec<i8> {
    // make sure v's destructor doesn't free the data
    let mut v = std::mem::ManuallyDrop::new(v);

    let p = v.as_mut_ptr();
    let len = v.len();
    let cap = v.capacity();

    unsafe { Vec::from_raw_parts(p as *mut i8, len, cap) }
}

pub(crate) fn new_exception<'local>(
    env: &mut JNIEnv<'local>,
    code: &str,
    message: &str,
) -> Result<JThrowable<'local>, jni::errors::Error> {
    let error_code_class = env.find_class("org/apache/opendal/exception/OpenDALErrorCode")?;
    let error_code_string = env.new_string(code)?;
    let error_code = env.call_static_method(
        error_code_class,
        "parse",
        "(Ljava/lang/String;)Lorg/apache/opendal/exception/OpenDALErrorCode;",
        &[JValue::Object(error_code_string.as_ref())],
    )?;

    let exception_class = env.find_class("org/apache/opendal/exception/OpenDALException")?;
    let exception = env.new_object(
        exception_class,
        "(Lorg/apache/opendal/exception/OpenDALErrorCode;Ljava/lang/String;)V",
        &[
            JValue::Object(error_code.l()?.as_ref()),
            JValue::Object(env.new_string(message)?.as_ref()),
        ],
    )?;
    Ok(JThrowable::from(exception))
}

pub(crate) unsafe fn complete_future(
    future: impl AsRef<JObject<'static>>,
    f: impl FnOnce(&mut JNIEnv) -> bool,
) {
    JENV.with(|cell| {
        let env_ptr = cell.borrow().unwrap();
        let mut env = JNIEnv::from_raw(env_ptr).unwrap();

        let result = if f(&mut env) { "TRUE" } else { "FALSE" };

        // build result
        let boolean_class = env.find_class("java/lang/Boolean").unwrap();
        let boolean = env
            .get_static_field(boolean_class, result, "Ljava/lang/Boolean;")
            .unwrap();

        // complete the java future
        let _ = env
            .call_method(
                future,
                "complete",
                "(Ljava/lang/Object;)Z",
                &[boolean.borrow()],
            )
            .unwrap();
    });
}
