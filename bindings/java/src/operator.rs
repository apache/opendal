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
use std::str::FromStr;

use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jboolean, jlong, jsize};
use jni::JNIEnv;

use opendal::{BlockingOperator, Operator, Scheme};

use crate::macros::*;
use crate::util;

#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_getOperator(
    mut env: JNIEnv,
    _class: JClass,
    input: JString,
    params: JObject,
) -> jlong {
    let input: String = util::jstring_into_string(&env, &input);

    let scheme = handle_opendal_result!(env, Scheme::from_str(&input), 0);
    let map = util::jobject_into_map(&mut env, &params);

    let result = build_operator(scheme, map);
    let operator = handle_opendal_result!(env, result, 0);
    Box::into_raw(Box::new(operator)) as jlong
}

fn build_operator(
    scheme: Scheme,
    map: HashMap<String, String>,
) -> Result<Operator, opendal::Error> {
    use opendal::services::*;

    let op = match scheme {
        Scheme::Azblob => Operator::from_map::<Azblob>(map)?.finish(),
        Scheme::Azdfs => Operator::from_map::<Azdfs>(map)?.finish(),
        Scheme::Fs => Operator::from_map::<Fs>(map)?.finish(),
        Scheme::Gcs => Operator::from_map::<Gcs>(map)?.finish(),
        Scheme::Ghac => Operator::from_map::<Ghac>(map)?.finish(),
        Scheme::Http => Operator::from_map::<Http>(map)?.finish(),
        Scheme::Ipmfs => Operator::from_map::<Ipmfs>(map)?.finish(),
        Scheme::Memory => Operator::from_map::<Memory>(map)?.finish(),
        Scheme::Obs => Operator::from_map::<Obs>(map)?.finish(),
        Scheme::Oss => Operator::from_map::<Oss>(map)?.finish(),
        Scheme::S3 => Operator::from_map::<S3>(map)?.finish(),
        Scheme::Webdav => Operator::from_map::<Webdav>(map)?.finish(),
        Scheme::Webhdfs => Operator::from_map::<Webhdfs>(map)?.finish(),

        _ => {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "Scheme not supported",
            ));
        }
    };

    Ok(op)
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_freeOperator(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut Operator,
) {
    // Take ownership of the pointer by wrapping it with a Box
    let _ = Box::from_raw(ptr);
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_write(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut BlockingOperator,
    file: JString,
    content: JByteArray,
) {
    let op = &mut *ptr;
    let file: String = util::jstring_into_string(&env, &file);

    let vec_result = env.convert_byte_array(content);
    let vec = handle_jni_result!(env, vec_result);

    let result = op.write(&file, vec);
    handle_opendal_result!(env, result);
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_writeAsync(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut Operator,
    file: JString,
    content: JByteArray,
    future: JObject,
) {
    let op = &mut *ptr;
    let file: String = util::jstring_into_string(&env, &file);
    let vec_result = env.convert_byte_array(content);
    let vec = handle_jni_result!(env, vec_result);

    // keep the future alive, so that we can complete it later
    // but this approach will be limited by global ref table size
    let ref_result = env.new_global_ref(future);
    let future = handle_jni_result!(env, ref_result);

    let x = async move {
        let op_result = op.write(&file, vec).await;
        util::complete_future(future, |env| {
            handle_opendal_result!(*env, op_result, false);
            true
        });
    };
    util::get_runtime().spawn(x);
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
) -> JByteArray<'local> {
    let op = &mut *ptr;
    let file = util::jstring_into_string(&env, &file);

    let content = handle_opendal_result!(env, op.read(&file), JByteArray::default());

    let byte_array = handle_jni_result!(
        env,
        env.new_byte_array(content.len() as jsize),
        JByteArray::default()
    );
    let result = env.set_byte_array_region(&byte_array, 0, &util::vec_u8_into_i8(content));
    handle_jni_result!(env, result, JByteArray::default());
    byte_array
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
    let file = util::jstring_into_string(&env, &file);
    let result = op.stat(&file);

    let ptr = handle_opendal_result!(env, result, 0 as jlong);
    Box::into_raw(Box::new(ptr)) as jlong
}

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_isFile(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut opendal::Metadata,
) -> jboolean {
    let metadata = &mut *ptr;
    metadata.is_file() as jboolean
}

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_getContentLength(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut opendal::Metadata,
) -> jlong {
    let metadata = &mut *ptr;
    metadata.content_length() as jlong
}

/// # Safety
///
/// This function should not be called before the Stat are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Metadata_freeMetadata(
    mut _env: JNIEnv,
    _class: JClass,
    ptr: *mut opendal::Metadata,
) {
    // Take ownership of the pointer by wrapping it with a Box
    let _ = Box::from_raw(ptr);
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
    let file: String = util::jstring_into_string(&env, &file);
    let result = op.delete(&file);
    handle_opendal_result!(env, result);
}
