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
use opendal::blocking;
use opendal::options;

use crate::convert::{
    bytes_to_jbytearray, jstring_to_string, offset_length_to_range, read_bool_field,
    read_int64_field, read_map_field, read_string_field,
};
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
    op: *mut blocking::Operator,
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
    op: *mut blocking::Operator,
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
    op: *mut blocking::Operator,
    path: JString,
    read_options: JObject,
) -> jbyteArray {
    intern_read(&mut env, &mut *op, path, read_options).unwrap_or_else(|e| {
        e.throw(&mut env);
        JByteArray::default().into_raw()
    })
}

fn intern_read(
    env: &mut JNIEnv,
    op: &mut blocking::Operator,
    path: JString,
    options: JObject,
) -> Result<jbyteArray> {
    let path = jstring_to_string(env, &path)?;

    let offset = read_int64_field(env, &options, "offset")?;
    let length = read_int64_field(env, &options, "length")?;

    let content = op.read_options(
        &path,
        options::ReadOptions {
            range: offset_length_to_range(offset, length)?.into(),
            ..Default::default()
        },
    )?;

    let result = bytes_to_jbytearray(env, content.to_bytes())?;
    Ok(result.into_raw())
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_write(
    mut env: JNIEnv,
    _: JClass,
    op: *mut blocking::Operator,
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
    op: &mut blocking::Operator,
    path: JString,
    content: JByteArray,
    options: JObject,
) -> Result<()> {
    let path = jstring_to_string(env, &path)?;
    let content = env.convert_byte_array(content)?;

    let append = read_bool_field(env, &options, "append")?;
    let content_type = read_string_field(env, &options, "contentType")?;
    let content_disposition = read_string_field(env, &options, "contentDisposition")?;
    let cache_control = read_string_field(env, &options, "cacheControl")?;
    let user_metadata = read_map_field(env, &options, "userMetadata")?;

    let _ = op.write_options(
        &path,
        content,
        options::WriteOptions {
            append,
            content_type,
            content_disposition,
            cache_control,
            user_metadata,
            ..Default::default()
        },
    )?;
    Ok(())
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_stat(
    mut env: JNIEnv,
    _: JClass,
    op: *mut blocking::Operator,
    path: JString,
) -> jobject {
    intern_stat(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::default().into_raw()
    })
}

fn intern_stat(env: &mut JNIEnv, op: &mut blocking::Operator, path: JString) -> Result<jobject> {
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
    op: *mut blocking::Operator,
    path: JString,
) {
    intern_delete(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_delete(env: &mut JNIEnv, op: &mut blocking::Operator, path: JString) -> Result<()> {
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
    op: *mut blocking::Operator,
    path: JString,
) {
    intern_create_dir(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_create_dir(env: &mut JNIEnv, op: &mut blocking::Operator, path: JString) -> Result<()> {
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
    op: *mut blocking::Operator,
    source_path: JString,
    target_path: JString,
) {
    intern_copy(&mut env, &mut *op, source_path, target_path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_copy(
    env: &mut JNIEnv,
    op: &mut blocking::Operator,
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
    op: *mut blocking::Operator,
    source_path: JString,
    target_path: JString,
) {
    intern_rename(&mut env, &mut *op, source_path, target_path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_rename(
    env: &mut JNIEnv,
    op: &mut blocking::Operator,
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
    op: *mut blocking::Operator,
    path: JString,
) {
    intern_remove_all(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_remove_all(env: &mut JNIEnv, op: &mut blocking::Operator, path: JString) -> Result<()> {
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
    op: *mut blocking::Operator,
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
    op: &mut blocking::Operator,
    path: JString,
    options: JObject,
) -> Result<jobjectArray> {
    let path = jstring_to_string(env, &path)?;
    let recursive = read_bool_field(env, &options, "recursive")?;

    let entries = op.list_options(
        &path,
        options::ListOptions {
            recursive,
            ..Default::default()
        },
    )?;

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
