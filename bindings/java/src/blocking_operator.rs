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

use std::str::FromStr;

use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;

use crate::convert;
use opendal::{BlockingOperator, Operator, Scheme};

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_Operator_constructor(
    mut env: JNIEnv,
    _: JClass,
    input: JString,
    params: JObject,
) -> jlong {
    let input: String = env
        .get_string(&input)
        .expect("cannot get java string")
        .into();

    let scheme = Scheme::from_str(&input).unwrap();

    let map = convert::jmap_to_hashmap(&mut env, &params);
    if let Ok(operator) = Operator::via_map(scheme, map) {
        Box::into_raw(Box::new(operator)) as jlong
    } else {
        env.exception_clear().expect("cannot clear exception");
        env.throw_new(
            "java/lang/IllegalArgumentException",
            "Unsupported operator.",
        )
        .expect("cannot throw exception");
        0 as jlong
    }
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_read<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ptr: *mut BlockingOperator,
    file: JString<'local>,
) -> JString<'local> {
    let op = &mut *ptr;
    let file: String = env
        .get_string(&file)
        .expect("cannot get java string!")
        .into();
    let content = String::from_utf8(op.read(&file).unwrap()).expect("cannot convert to string");
    env.new_string(content).expect("cannot create java string")
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_disposeInternal(
    _: JNIEnv,
    _: JClass,
    ptr: *mut BlockingOperator,
) {
    drop(Box::from_raw(ptr));
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_write(
    mut env: JNIEnv,
    _: JClass,
    ptr: *mut BlockingOperator,
    file: JString,
    content: JString,
) {
    let op = &mut *ptr;
    let file: String = env
        .get_string(&file)
        .expect("cannot get java string!")
        .into();
    let content: String = env
        .get_string(&content)
        .expect("cannot get java string!")
        .into();
    op.write(&file, content).unwrap();
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_stat(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut BlockingOperator,
    file: JString,
) -> jlong {
    let op = &mut *ptr;
    let file: String = env
        .get_string(&file)
        .expect("cannot get java string!")
        .into();
    let result = op.stat(&file);
    if let Err(error) = result {
        let exception = convert::error_to_exception(&mut env, error).unwrap();
        env.throw(exception).unwrap();
        return 0 as jlong;
    }
    Box::into_raw(Box::new(result.unwrap())) as jlong
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_delete<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    ptr: *mut BlockingOperator,
    file: JString<'local>,
) {
    let op = &mut *ptr;
    let file: String = env
        .get_string(&file)
        .expect("cannot get java string!")
        .into();
    op.delete(&file).unwrap();
}
