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

use jni::JNIEnv;
use jni::objects::JByteArray;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JString;
use jni::objects::JValue;
use jni::objects::JValueOwned;
use jni::sys::jlong;
use jni::sys::jobject;
use jni::sys::jsize;
use opendal::Entry;
use opendal::Operator;
use opendal::Scheme;
use opendal::blocking;

use crate::Result;
use crate::convert::{
    bytes_to_jbytearray, jmap_to_hashmap, jstring_to_string, offset_length_to_range,
    read_int64_field,
};
use crate::executor::Executor;
use crate::executor::executor_or_default;
use crate::executor::get_current_env;
use crate::make_metadata;
use crate::make_operator_info;
use crate::make_presigned_request;
use crate::{make_entry, make_list_options, make_stat_options, make_write_options};

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_AsyncOperator_constructor(
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
    let scheme = Scheme::from_str(jstring_to_string(env, &scheme)?.as_str())?;
    let map = jmap_to_hashmap(env, &map)?;
    let op = Operator::via_iter(scheme, map)?;
    Ok(Box::into_raw(Box::new(op)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_duplicate(
    _: JNIEnv,
    _: JClass,
    op: *mut Operator,
) -> jlong {
    let op = unsafe { &mut *op };
    Box::into_raw(Box::new(op.clone())) as jlong
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_disposeInternal(
    _: JNIEnv,
    _: JObject,
    op: *mut Operator,
) {
    unsafe {
        drop(Box::from_raw(op));
    }
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
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

    let write_opts = make_write_options(env, &options)?;
    let path = jstring_to_string(env, &path)?;
    let content = env.convert_byte_array(content)?;

    executor_or_default(env, executor)?.spawn(async move {
        let result = op
            .write_options(&path, content, write_opts)
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
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_stat(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    stat_options: JObject,
) -> jlong {
    intern_stat(&mut env, op, executor, path, stat_options).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_stat(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let id = request_id(env)?;

    let path = jstring_to_string(env, &path)?;
    let stat_opts = make_stat_options(env, &options)?;

    executor_or_default(env, executor)?.spawn(async move {
        let metadata = op.stat_options(&path, stat_opts).await.map_err(Into::into);
        let mut env = unsafe { get_current_env() };
        let result = metadata.and_then(|metadata| make_metadata(&mut env, metadata));
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_read(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    read_options: JObject,
) -> jlong {
    intern_read(&mut env, op, executor, path, read_options).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_read(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
    path: JString,
    options: JObject,
) -> Result<jlong> {
    // Prepare inputs before spawning
    let id = request_id(env)?;
    let path_str = jstring_to_string(env, &path)?;
    let offset = read_int64_field(env, &options, "offset")?;
    let length = read_int64_field(env, &options, "length")?;
    let range = offset_length_to_range(offset, length)?;

    // Clone operator handle to move into the task
    let op_cloned = unsafe { &*op }.clone();

    executor_or_default(env, executor)?.spawn(async move {
        let mut read_op = op_cloned.read_with(&path_str);
        read_op = read_op.range(range);
        let result = read_op.await.map_err(Into::into);

        let mut env = unsafe { get_current_env() };
        let result = result.and_then(|bs| bytes_to_jbytearray(&mut env, bs.to_bytes()));
        complete_future(id, result.map(|bs| JValueOwned::Object(bs.into())))
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_AsyncOperator_makeBlockingOp(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    executor: *const Executor,
) -> jlong {
    intern_make_blocking_op(&mut env, op, executor).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_make_blocking_op(
    env: &mut JNIEnv,
    op: *mut Operator,
    executor: *const Executor,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let op = executor_or_default(env, executor)?
        .enter_with(move || blocking::Operator::new(op.clone()))?;
    Ok(Box::into_raw(Box::new(op)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
#[unsafe(no_mangle)]
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
        let result = async {
            let lister = op.lister_with(&path).recursive(true).await?;
            op.delete_try_stream(lister).await
        }
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
#[unsafe(no_mangle)]
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
    let list_opts = make_list_options(env, &options)?;
    executor_or_default(env, executor)?.spawn(async move {
        let entries = op.list_options(&path, list_opts).await.map_err(Into::into);
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
#[unsafe(no_mangle)]
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
        let result = op.presign_read(&path, expire).await.map_err(Into::into);
        let mut env = unsafe { get_current_env() };
        let result = result.and_then(|req| make_presigned_request(&mut env, req));
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}
/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
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
        let result = op.presign_write(&path, expire).await.map_err(Into::into);
        let mut env = unsafe { get_current_env() };
        let result = result.and_then(|req| make_presigned_request(&mut env, req));
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
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
        let result = op.presign_stat(&path, expire).await.map_err(Into::into);
        let mut env = unsafe { get_current_env() };
        let result = result.and_then(|req| make_presigned_request(&mut env, req));
        complete_future(id, result.map(JValueOwned::Object))
    });

    Ok(id)
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
