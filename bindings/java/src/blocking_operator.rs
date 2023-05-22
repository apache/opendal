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

use opendal::Result;
use opendal::{BlockingOperator, Operator, Scheme};

use crate::{convert, or_throw};

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_Operator_constructor(
    mut env: JNIEnv,
    _: JClass,
    scheme: JString,
    map: JObject,
) -> jlong {
    let res = intern_constructor(&mut env, scheme, map);
    or_throw(&mut env, res)
}

fn intern_constructor(env: &mut JNIEnv, scheme: JString, map: JObject) -> Result<jlong> {
    let scheme = {
        let res = env.get_string(&scheme).map_err(convert::error_to_error)?;
        let res = res.to_str().map_err(convert::error_to_error)?;
        Scheme::from_str(res)?
    };
    let map = convert::jmap_to_hashmap(env, &map).map_err(convert::error_to_error)?;
    let op = Operator::via_map(scheme, map)?;
    Ok(Box::into_raw(Box::new(op.blocking())) as jlong)
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
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_read<'local>(
    mut env: JNIEnv<'local>,
    _: JClass<'local>,
    op: *mut BlockingOperator,
    file: JString<'local>,
) -> JString<'local> {
    let res = intern_read(&mut env, &mut *op, file);
    or_throw(&mut env, res)
}

fn intern_read<'local>(
    env: &mut JNIEnv<'local>,
    op: &mut BlockingOperator,
    file: JString<'local>,
) -> Result<JString<'local>> {
    let content = {
        let file = env.get_string(&file).map_err(convert::error_to_error)?;
        let file = file.to_str().map_err(convert::error_to_error)?;
        let res = op.read(file)?;
        let res = String::from_utf8(res).map_err(convert::error_to_error)?;
        env.new_string(res).map_err(convert::error_to_error)?
    };
    Ok(content)
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_write(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    file: JString,
    content: JString,
) {
    let res = intern_write(&mut env, &mut *op, file, content);
    or_throw(&mut env, res)
}

fn intern_write(
    env: &mut JNIEnv,
    op: &mut BlockingOperator,
    file: JString,
    content: JString,
) -> Result<()> {
    let file = env.get_string(&file).map_err(convert::error_to_error)?;
    let file = file.to_str().map_err(convert::error_to_error)?;
    let content = env.get_string(&content).map_err(convert::error_to_error)?;
    let content = content.to_str().map_err(convert::error_to_error)?;
    op.write(file, content.to_string())
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_stat(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    file: JString,
) -> jlong {
    let res = intern_stat(&mut env, &mut *op, file);
    or_throw(&mut env, res)
}

fn intern_stat(env: &mut JNIEnv, op: &mut BlockingOperator, file: JString) -> Result<jlong> {
    let file = env.get_string(&file).map_err(convert::error_to_error)?;
    let file = file.to_str().map_err(convert::error_to_error)?;
    let metadata = op.stat(file)?;
    Ok(Box::into_raw(Box::new(metadata)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_delete(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    file: JString,
) {
    let res = intern_delete(&mut env, &mut *op, file);
    or_throw(&mut env, res)
}

fn intern_delete(env: &mut JNIEnv, op: &mut BlockingOperator, file: JString) -> Result<()> {
    let file = env.get_string(&file).map_err(convert::error_to_error)?;
    let file = file.to_str().map_err(convert::error_to_error)?;
    op.delete(file)
}
