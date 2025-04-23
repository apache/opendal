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
use std::time::Duration;

use jni::objects::JByteArray;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::JValue;
use jni::objects::JValueOwned;
use jni::sys::jlong;
use jni::sys::jobject;
use jni::sys::jsize;
use jni::JNIEnv;
use opendal::layers::BlockingLayer;
use opendal::raw::PresignedRequest;
use opendal::Entry;
use opendal::Operator;
use opendal::Scheme;

use crate::convert::{jmap_to_hashmap, read_map_field, read_string_field};
use crate::convert::{jstring_to_string, read_bool_field};
use crate::executor::executor_or_default;
use crate::executor::get_current_env;
use crate::executor::Executor;
use crate::make_entry;
use crate::make_metadata;
use crate::make_operator_info;
use crate::make_presigned_request;
use crate::Result;

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_AsyncOperator_constructor(
    mut env: JNIEnv,
    _: JClass,
    executor: *const Executor,
    scheme: JString,
    map: JObject,
) -> jlong {
    intern_constructor(&mut env, executor, scheme, map).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_constructor(
    env: &mut JNIEnv,
    executor: *const Executor,
    scheme: JString,
    map: JObject,
) -> Result<jlong> {
    let scheme = Scheme::from_str(jstring_to_string(env, &scheme)?.as_str())?;
    let map = jmap_to_hashmap(env, &map)?;
    let mut op = Operator::via_iter(scheme, map)?;
    if !op.info().full_capability().blocking {
        let layer = executor_or_default(env, executor)?.enter_with(BlockingLayer::create)?;
        op = op.layer(layer);
    }
    Ok(Box::into_raw(Box::new(op)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_duplicate(
    _: JNIEnv,
    _: JClass,
    op: *mut Operator,
) -> jlong {
    let op = &mut *op;
    Box::into_raw(Box::new(op.clone())) as jlong
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_disposeInternal(
    _: JNIEnv,
    _: JObject,
    op: *mut Operator,
) {
    drop(Box::from_raw(op));
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_write(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    content: JByteArray,
    write_options: JObject,
) -> jlong {
    intern_write(&mut env, op, executor, path, content, write_options).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_write(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    content: JByteArray,
    options: JObject,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;
    let content = env.convert_byte_array(content)?;
    let content_type = read_string_field(env, &options, "contentType")?;
    let content_disposition = read_string_field(env, &options, "contentDisposition")?;
    let content_encoding = read_string_field(env, &options, "contentEncoding")?;
    let cache_control = read_string_field(env, &options, "cacheControl")?;
    let if_match = read_string_field(env, &options, "ifMatch")?;
    let if_none_match = read_string_field(env, &options, "ifNoneMatch")?;
    let append = read_bool_field(env, &options, "append")?;
    let if_not_exists = read_bool_field(env, &options, "ifNotExists")?;
    let user_metadata = read_map_field(env, &options, "userMetadata")?;

    let mut write_op = op.write_with(&path, content);
    if let Some(content_type) = content_type {
        write_op = write_op.content_type(&content_type);
    }
    if let Some(content_disposition) = content_disposition {
        write_op = write_op.content_disposition(&content_disposition);
    }
    if let Some(content_encoding) = content_encoding {
        write_op = write_op.content_encoding(&content_encoding);
    }
    if let Some(cache_control) = cache_control {
        write_op = write_op.cache_control(&cache_control);
    }
    if let Some(if_match) = if_match {
        write_op = write_op.if_match(&if_match);
    }
    if let Some(if_none_match) = if_none_match {
        write_op = write_op.if_none_match(&if_none_match);
    }
    if let Some(user_metadata) = user_metadata {
        write_op = write_op.user_metadata(user_metadata);
    }
    write_op = write_op.if_not_exists(if_not_exists);
    write_op = write_op.append(append);

    executor_or_default(env, executor)?.spawn(async move {
        let result = write_op
            .await
            .map(|_| JValueOwned::Void)
            .map_err(Into::into);
        complete_future(id, result)
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_append(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    content: JByteArray,
) -> jlong {
    intern_append(&mut env, op, executor, path, content).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_append(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    content: JByteArray,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;
    let content = env.convert_byte_array(content)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = op
            .write_with(&path, content)
            .append(true)
            .await
            .map(|_| JValueOwned::Void)
            .map_err(Into::into);
        complete_future(id, result)
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_stat(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> jlong {
    intern_stat(&mut env, op, executor, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_stat(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = do_stat(op, path).await;
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

async fn do_stat<'local>(op: &mut Operator, path: String) -> Result<JObject<'local>> {
    let metadata = op.stat(&path).await?;
    let mut env = unsafe { get_current_env() };
    make_metadata(&mut env, metadata)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_read(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> jlong {
    intern_read(&mut env, op, executor, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_read(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = do_read(op, path).await;
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_read_with_offset(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    offset: jlong,
    len: jlong,
    path: JString,
) -> jlong {
    intern_read_with_offset(&mut env, op, executor, offset, len, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_read_with_offset(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    offset: jlong,
    len: jlong,
    path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = do_read_with_offset(op, offset, len, path).await;
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_read_with_options(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    read_options: JObject,
    path: JString,
) -> jlong {
    intern_read_with_options(&mut env, op, executor, read_options, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_read_with_options(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    read_options: JObject,
    path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;
    let path = jstring_to_string(env, &path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = do_read_with_options(op, read_options, path).await;
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

async fn do_read<'local>(op: &mut Operator, path: String) -> Result<JObject<'local>> {
    let content = op.read(&path).await?.to_bytes();

    let env = unsafe { get_current_env() };
    let result = env.byte_array_from_slice(&content)?;
    Ok(result.into())
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_delete(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> jlong {
    intern_delete(&mut env, op, executor, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

async fn do_read_with_offset<'local>(op: &mut Operator, offset: jlong, len: jlong, path: String) -> Result<JObject<'local>> {
    let offset = offset as u64;
    let len = len as u64;

    let buffer = op.read_with(&path).range(offset..(offset + len)).await?.to_bytes();
    let env = unsafe { get_current_env() };
    let result = env.byte_array_from_slice(&buffer)?;
    Ok(result.into())
}

async fn do_read_with_options<'local>(op: &mut Operator, read_options: JObject, path: String) -> Result<JObject<'local>> {
    let offset = env.get_field(read_options, "offset", "J")?.j()?;
    let length = {
        let jlong = env.get_field(read_options, "length", "J")?.j()?;
        if jlong == -1 {
            None
        } else {
            Some(jlong as u64)
        }
    };
    let buffer_size = env.get_field(read_options, "bufferSize", "I")?.i()? as usize;
    let charset_name = env.get_field(read_options, "charset", "Ljava/lang/String;")?;
    let charset = if !charset_name.is_null() {
        let charset_str = env.get_string(charset_name.into())?;
        Encoding::for_label(charset_str.to_bytes()).unwrap_or(UTF_8)
    } else {
        UTF_8
    };
    let skip_new_line = env.get_field(read_options, "skipNewLine", "Z")?.z()?;

    let options = ReadOptions::builder()
        .offset(offset)
        .length(length)
        .buffer_size(buffer_size)
        .charset(charset)
        .skip_new_line(skip_new_line)
        .build()
        .map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Failed to build ReadOptions: {}", e),
            )
        })?;

    let content = op.read_with(&path, &options).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("Failed to read with options: {}", e),
        )
    })?;

    let array = env.new_byte_array(content.len() as i32)?;
    env.set_byte_array_region(array, 0, &content)?;

    Ok(array)
}

fn intern_delete(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = op
            .delete(&path)
            .await
            .map(|_| JValueOwned::Void)
            .map_err(Into::into);
        complete_future(id, result)
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_makeBlockingOp(
    _: JNIEnv,
    _: JClass,
    op: *mut Operator,
) -> jlong {
    let op = unsafe { &mut *op };
    Box::into_raw(Box::new(op.blocking())) as jlong
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_makeOperatorInfo(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
) -> jobject {
    intern_make_operator_info(&mut env, op).unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::default().into_raw()
    })
}

fn intern_make_operator_info(env: &mut JNIEnv, op: *mut Operator) -> Result<jobject> {
    let op = unsafe { &mut *op };
    Ok(make_operator_info(env, op.info())?.into_raw())
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_createDir(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> jlong {
    intern_create_dir(&mut env, op, executor, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_create_dir(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = op
            .create_dir(&path)
            .await
            .map(|_| JValueOwned::Void)
            .map_err(Into::into);
        complete_future(id, result)
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_copy(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    source_path: JString,
    target_path: JString,
) -> jlong {
    intern_copy(&mut env, op, executor, source_path, target_path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_copy(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    source_path: JString,
    target_path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let source_path = jstring_to_string(env, &source_path)?;
    let target_path = jstring_to_string(env, &target_path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = op
            .copy(&source_path, &target_path)
            .await
            .map(|_| JValueOwned::Void)
            .map_err(Into::into);
        complete_future(id, result)
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_rename(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    source_path: JString,
    target_path: JString,
) -> jlong {
    intern_rename(&mut env, op, executor, source_path, target_path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_rename(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    source_path: JString,
    target_path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let source_path = jstring_to_string(env, &source_path)?;
    let target_path = jstring_to_string(env, &target_path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = op
            .rename(&source_path, &target_path)
            .await
            .map(|_| JValueOwned::Void)
            .map_err(Into::into);
        complete_future(id, result)
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_removeAll(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> jlong {
    intern_remove_all(&mut env, op, executor, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_remove_all(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = op
            .remove_all(&path)
            .await
            .map(|_| JValueOwned::Void)
            .map_err(Into::into);
        complete_future(id, result)
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_list(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> jlong {
    intern_list(&mut env, op, executor, path, options).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_list(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;
    let recursive = read_bool_field(env, &options, "recursive")?;

    let mut list_op = op.list_with(&path);
    list_op = list_op.recursive(recursive);

    executor_or_default(env, executor)?.spawn(async move {
        let entries = list_op.await.map_err(Into::into);
        let result = make_entries(entries);
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

fn make_entries<'local>(entries: Result<Vec<Entry>>) -> Result<JObject<'local>> {
    let entries = entries?;

    let mut env = unsafe { get_current_env() };
    let jarray = env.new_object_array(
        entries.len() as jsize,
        "org/apache/opendal/Entry",
        JObject::null(),
    )?;

    for (idx, entry) in entries.into_iter().enumerate() {
        let entry = make_entry(&mut env, entry)?;
        env.set_object_array_element(&jarray, idx as jsize, entry)?;
    }

    Ok(jarray.into())
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_presignRead(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    expire: jlong,
) -> jlong {
    intern_presign_read(&mut env, op, executor, path, expire).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_presign_read(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    expire: jlong,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;
    let expire = Duration::from_nanos(expire as u64);

    executor_or_default(env, executor)?.spawn(async move {
        let result = do_presign_read(op, path, expire).await;
        let mut env = unsafe { get_current_env() };
        let result = result.and_then(|req| make_presigned_request(&mut env, req));
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

async fn do_presign_read(
    op: &mut Operator,
    path: String,
    expire: Duration,
) -> Result<PresignedRequest> {
    Ok(op.presign_read(&path, expire).await?)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_presignWrite(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    expire: jlong,
) -> jlong {
    intern_presign_write(&mut env, op, executor, path, expire).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_presign_write(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    expire: jlong,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;
    let expire = Duration::from_nanos(expire as u64);

    executor_or_default(env, executor)?.spawn(async move {
        let result = do_presign_write(op, path, expire).await;
        let mut env = unsafe { get_current_env() };
        let result = result.and_then(|req| make_presigned_request(&mut env, req));
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

async fn do_presign_write(
    op: &mut Operator,
    path: String,
    expire: Duration,
) -> Result<PresignedRequest> {
    Ok(op.presign_write(&path, expire).await?)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_presignStat(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    expire: jlong,
) -> jlong {
    intern_presign_stat(&mut env, op, executor, path, expire).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_presign_stat(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    expire: jlong,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;
    let expire = Duration::from_nanos(expire as u64);

    executor_or_default(env, executor)?.spawn(async move {
        let result = do_presign_stat(op, path, expire).await;
        let mut env = unsafe { get_current_env() };
        let result = result.and_then(|req| make_presigned_request(&mut env, req));
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

async fn do_presign_stat(
    op: &mut Operator,
    path: String,
    expire: Duration,
) -> Result<PresignedRequest> {
    Ok(op.presign_stat(&path, expire).await?)
}

fn make_object<'local>(
    env: &mut JNIEnv<'local>,
    value: JValueOwned<'local>,
) -> Result<JObject<'local>> {
    let o = match value {
        JValueOwned::Object(o) => o,
        JValueOwned::Byte(_) => env.new_object("java/lang/Long", "(B)V", &[value.borrow()])?,
        JValueOwned::Char(_) => env.new_object("java/lang/Char", "(C)V", &[value.borrow()])?,
        JValueOwned::Short(_) => env.new_object("java/lang/Short", "(S)V", &[value.borrow()])?,
        JValueOwned::Int(_) => env.new_object("java/lang/Integer", "(I)V", &[value.borrow()])?,
        JValueOwned::Long(_) => env.new_object("java/lang/Long", "(J)V", &[value.borrow()])?,
        JValueOwned::Bool(_) => env.new_object("java/lang/Boolean", "(Z)V", &[value.borrow()])?,
        JValueOwned::Float(_) => env.new_object("java/lang/Float", "(F)V", &[value.borrow()])?,
        JValueOwned::Double(_) => env.new_object("java/lang/Double", "(D)V", &[value.borrow()])?,
        JValueOwned::Void => JObject::null(),
    };
    Ok(o)
}

fn complete_future(id: jlong, result: Result<JValueOwned>) {
    try_complete_future(id, result).expect("complete future must succeed");
}

fn try_complete_future(id: jlong, result: Result<JValueOwned>) -> Result<()> {
    let mut env = unsafe { get_current_env() };
    let future = get_future(&mut env, id)?;
    match result {
        Ok(result) => {
            let result = make_object(&mut env, result)?;
            env.call_method(
                future,
                "complete",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(&result)],
            )?
        }
        Err(err) => {
            let exception = err.to_exception(&mut env)?;
            env.call_method(
                future,
                "completeExceptionally",
                "(Ljava/lang/Throwable;)Z",
                &[JValue::Object(&exception)],
            )?
        }
    };
    Ok(())
}

fn request_id(env: &mut JNIEnv) -> Result<jlong> {
    Ok(env
        .call_static_method(
            "org/apache/opendal/AsyncOperator$AsyncRegistry",
            "requestId",
            "()J",
            &[],
        )?
        .j()?)
}

fn get_future<'local>(env: &mut JNIEnv<'local>, id: jlong) -> Result<JObject<'local>> {
    Ok(env
        .call_static_method(
            "org/apache/opendal/AsyncOperator$AsyncRegistry",
            "get",
            "(J)Ljava/util/concurrent/CompletableFuture;",
            &[JValue::Long(id)],
        )?
        .l()?)
}
