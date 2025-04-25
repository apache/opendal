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

use anyhow::anyhow;
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

use crate::convert::{jstring_to_string, read_bool_field, read_map_field, read_string_field};
use crate::make_entry;
use crate::make_metadata;
use crate::options::ReadOptions;
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

// Rust FFI function for OpenDAL's sync `read_with_offset`
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_read_with_offset(
    mut jni_env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    offset: jlong,
    len: jlong,
    path: JString,
) -> jbyteArray {
    intern_read_with_offset(&mut jni_env, &mut *op, offset, len, path).unwrap_or_else(|e| {
        e.throw(&mut jni_env);
        JByteArray::default().into_raw()
    })
}

fn intern_read_with_offset(
    jni_env: &mut JNIEnv,
    op: &mut BlockingOperator,
    offset: jlong,
    len: jlong,
    path: JString,
) -> Result<jbyteArray> {
    let path = jstring_to_string(jni_env, &path)?;

    let offset = offset as u64;
    let len = len as u64;
    let content = op.read_with(&path).range(offset..(offset + len)).call()?;
    let buffer = content.to_bytes();
    let result = jni_env.byte_array_from_slice(&buffer)?;

    Ok(result.into_raw())
}

// Rust FFI function for OpenDAL's sync `read_with_offset`
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_read_with_options(
    mut jni_env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    read_options: JObject,
    path: JString,
) -> jbyteArray {
    crate::operator::intern_read_with_options(&mut jni_env, &mut *op, read_options, path)
        .unwrap_or_else(|e| {
            e.throw(&mut jni_env);
            JByteArray::default().into_raw()
        })
}

fn intern_read_with_options(
    jni_env: &mut JNIEnv,
    op: &mut BlockingOperator,
    read_options: JObject,
    path: JString,
) -> Result<jbyteArray> {
    let cloned_options = read_options.clone();
    let offset = jni_env.get_field(cloned_options, "offset", "J")?.j()?;
    let length = {
        let jlong = jni_env.get_field(cloned_options, "length", "J")?.j()?;
        if jlong == -1 {
            None
        } else {
            Some(jlong as u64)
        }
    };
    let buffer_size = jni_env.get_field(cloned_options, "bufferSize", "I")?.i()? as usize;

    let path = jstring_to_string(jni_env, &path)?;

    let mut content = op.read_with(&path).chunk(buffer_size);
    if let Some(len) = length {
        content = content.range(offset..(offset + len));
    } else {
        content = content.range(offset..);
    }

    let array = jni_env.new_byte_array(content.len() as i32)?;
    jni_env.set_byte_array_region(array, 0, &content)?;

    Ok(array.clone().into_raw())
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
    options: JObject,
) -> Result<()> {
    let path = jstring_to_string(env, &path)?;
    let content = env.convert_byte_array(content)?;

    let content_type = read_string_field(env, &options, "contentType")?;
    let content_disposition = read_string_field(env, &options, "contentDisposition")?;
    let cache_control = read_string_field(env, &options, "cacheControl")?;
    let user_metadata = read_map_field(env, &options, "userMetadata")?;
    let append = read_bool_field(env, &options, "append")?;

    let mut write_op = op.write_with(&path, content);
    if let Some(content_type) = content_type {
        write_op = write_op.content_type(&content_type);
    }
    if let Some(content_disposition) = content_disposition {
        write_op = write_op.content_disposition(&content_disposition);
    }
    if let Some(cache_control) = cache_control {
        write_op = write_op.cache_control(&cache_control);
    }
    if let Some(user_metadata) = user_metadata {
        write_op = write_op.user_metadata(user_metadata);
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
    let recursive = read_bool_field(env, &options, "recursive")?;

    let mut list_op = op.list_with(&path);
    list_op = list_op.recursive(recursive);

    let entries = list_op.call()?;

    let jarray = env.new_object_array(
        entries.len() as jsize,
        "org/apache/opendal/Entry",
        JObject::null(),
    )?;

    for (idx, entry) in entries.into_iter().enumerate() {
        let entry = make_entry(env, entry)?;
        env.set_object_array_element(&jarray, idx as jsize, entry)?;
    }

    Ok(jarray.into_raw())
}
