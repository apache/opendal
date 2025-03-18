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

use jni::objects::JByteArray;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JString;
use jni::sys::jbyteArray;
use jni::sys::jlong;
use jni::sys::jobject;
use jni::sys::jobjectArray;
use jni::sys::jsize;
use jni::JNIEnv;
use opendal::BlockingOperator;

use crate::convert::{get_optional_string_from_object, jstring_to_string};
use crate::make_entry;
use crate::make_metadata;
use crate::Result;

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_disposeInternal(
    _: JNIEnv,
    _: JObject,
    op: *mut BlockingOperator,
) {
    drop(Box::from_raw(op));
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_duplicate(
    _: JNIEnv,
    _: JObject,
    op: *mut BlockingOperator,
) -> jlong {
    let op = &mut *op;
    Box::into_raw(Box::new(op.clone())) as jlong
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_read(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) -> jbyteArray {
    intern_read(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        JByteArray::default().into_raw()
    })
}

fn intern_read(env: &mut JNIEnv, op: &mut BlockingOperator, path: JString) -> Result<jbyteArray> {
    let path = jstring_to_string(env, &path)?;
    let content = op.read(&path)?.to_bytes();
    let result = env.byte_array_from_slice(&content)?;
    Ok(result.into_raw())
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_write(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
    content: JByteArray,
    write_options: JObject,
) {
    intern_write(&mut env, &mut *op, path, content, write_options).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_write(
    env: &mut JNIEnv,
    op: &mut BlockingOperator,
    path: JString,
    content: JByteArray,
    write_options: JObject,
) -> Result<()> {
    let path = jstring_to_string(env, &path)?;
    let content = env.convert_byte_array(content)?;

    let content_type = get_optional_string_from_object(env, &write_options, "getContentType")?;
    let content_disposition =
        get_optional_string_from_object(env, &write_options, "getContentDisposition")?;
    let cache_control = get_optional_string_from_object(env, &write_options, "getCacheControl")?;
    let append = env
        .call_method(&write_options, "isAppend", "()Z", &[])?
        .z()?;

    let mut write_op = op.write_with(&path, content);
    if let Some(ct) = content_type {
        write_op = write_op.content_type(&ct);
    }
    if let Some(cd) = content_disposition {
        write_op = write_op.content_disposition(&cd);
    }
    if let Some(cc) = cache_control {
        write_op = write_op.cache_control(&cc);
    }
    write_op = write_op.append(append);
    Ok(write_op.call().map(|_| ())?)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_stat(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) -> jobject {
    intern_stat(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::default().into_raw()
    })
}

fn intern_stat(env: &mut JNIEnv, op: &mut BlockingOperator, path: JString) -> Result<jobject> {
    let path = jstring_to_string(env, &path)?;
    let metadata = op.stat(&path)?;
    Ok(make_metadata(env, metadata)?.into_raw())
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_delete(
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
    let path = jstring_to_string(env, &path)?;
    Ok(op.delete(&path)?)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_createDir(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) {
    intern_create_dir(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_create_dir(env: &mut JNIEnv, op: &mut BlockingOperator, path: JString) -> Result<()> {
    let path = jstring_to_string(env, &path)?;
    Ok(op.create_dir(&path)?)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_copy(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    source_path: JString,
    target_path: JString,
) {
    intern_copy(&mut env, &mut *op, source_path, target_path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_copy(
    env: &mut JNIEnv,
    op: &mut BlockingOperator,
    source_path: JString,
    target_path: JString,
) -> Result<()> {
    let source_path = jstring_to_string(env, &source_path)?;
    let target_path = jstring_to_string(env, &target_path)?;

    Ok(op.copy(&source_path, &target_path)?)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_rename(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    source_path: JString,
    target_path: JString,
) {
    intern_rename(&mut env, &mut *op, source_path, target_path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_rename(
    env: &mut JNIEnv,
    op: &mut BlockingOperator,
    source_path: JString,
    target_path: JString,
) -> Result<()> {
    let source_path = jstring_to_string(env, &source_path)?;
    let target_path = jstring_to_string(env, &target_path)?;

    Ok(op.rename(&source_path, &target_path)?)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_removeAll(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) {
    intern_remove_all(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_remove_all(env: &mut JNIEnv, op: &mut BlockingOperator, path: JString) -> Result<()> {
    let path = jstring_to_string(env, &path)?;

    Ok(op.remove_all(&path)?)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_list(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
    options: JObject,
) -> jobjectArray {
    intern_list(&mut env, &mut *op, path, options).unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::default().into_raw()
    })
}

fn intern_list(
    env: &mut JNIEnv,
    op: &mut BlockingOperator,
    path: JString,
    options: JObject,
) -> Result<jobjectArray> {
    let path = jstring_to_string(env, &path)?;

    let mut list_op = op.list_with(&path);
    if env.call_method(&options, "isRecursive", "()Z", &[])?.z()? {
        list_op = list_op.recursive(true);
    }

    let obs = list_op.call()?;

    let jarray = env.new_object_array(
        obs.len() as jsize,
        "org/apache/opendal/Entry",
        JObject::null(),
    )?;

    for (idx, entry) in obs.iter().enumerate() {
        let entry = make_entry(env, entry.to_owned())?;
        env.set_object_array_element(&jarray, idx as jsize, entry)?;
    }

    Ok(jarray.into_raw())
}
