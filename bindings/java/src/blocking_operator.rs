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

use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::{JByteArray, JClass};
use jni::sys::jlong;
use jni::sys::jstring;
use jni::JNIEnv;
use opendal::BlockingOperator;
use opendal::Operator;
use opendal::Scheme;

use crate::jmap_to_hashmap;
use crate::Result;

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_BlockingOperator_constructor(
    mut env: JNIEnv,
    _: JClass,
    scheme: JString,
    map: JObject,
) -> jlong {
    intern_constructor(&mut env, scheme, map).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_constructor(env: &mut JNIEnv, scheme: JString, map: JObject) -> Result<jlong> {
    let scheme = Scheme::from_str(env.get_string(&scheme)?.to_str()?)?;
    let map = jmap_to_hashmap(env, &map)?;
    let op = Operator::via_map(scheme, map)?;
    Ok(Box::into_raw(Box::new(op.blocking())) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_BlockingOperator_disposeInternal(
    _: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
) {
    drop(Box::from_raw(op));
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_BlockingOperator_read(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) -> jstring {
    intern_read(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::null().into_raw()
    })
}

fn intern_read(env: &mut JNIEnv, op: &mut BlockingOperator, path: JString) -> Result<jstring> {
    let path = env.get_string(&path)?;
    let content = String::from_utf8(op.read(path.to_str()?)?)?;
    Ok(env.new_string(content)?.into_raw())
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_BlockingOperator_write(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
    content: JByteArray,
) {
    intern_write(&mut env, &mut *op, path, content).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_write(
    env: &mut JNIEnv,
    op: &mut BlockingOperator,
    path: JString,
    content: JByteArray,
) -> Result<()> {
    let path = env.get_string(&path)?;
    let content = env.convert_byte_array(content)?;
    Ok(op.write(path.to_str()?, content)?)
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_BlockingOperator_stat(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) -> jlong {
    intern_stat(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_stat(env: &mut JNIEnv, op: &mut BlockingOperator, path: JString) -> Result<jlong> {
    let path = env.get_string(&path)?;
    let metadata = op.stat(path.to_str()?)?;
    Ok(Box::into_raw(Box::new(metadata)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_BlockingOperator_delete(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) {
    intern_delete(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_delete(env: &mut JNIEnv, op: &mut BlockingOperator, path: JString) -> Result<()> {
    let path = env.get_string(&path)?;
    Ok(op.delete(path.to_str()?)?)
}
