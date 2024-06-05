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
use jni::objects::JString;
use jni::sys::jlong;
use jni::JNIEnv;
use opendal::BlockingOperator;
use opendal::BlockingWriter;

use crate::convert::jstring_to_string;

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorOutputStream_constructWriter(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) -> jlong {
    intern_construct_write(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_construct_write(
    env: &mut JNIEnv,
    op: &mut BlockingOperator,
    path: JString,
) -> crate::Result<jlong> {
    let path = jstring_to_string(env, &path)?;
    let writer = op.writer(&path)?;
    Ok(Box::into_raw(Box::new(writer)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorOutputStream_disposeWriter(
    mut env: JNIEnv,
    _: JClass,
    writer: *mut BlockingWriter,
) {
    let mut writer = Box::from_raw(writer);
    intern_dispose_write(&mut writer).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_dispose_write(writer: &mut BlockingWriter) -> crate::Result<()> {
    writer.close()?;
    Ok(())
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorOutputStream_writeBytes(
    mut env: JNIEnv,
    _: JClass,
    writer: *mut BlockingWriter,
    content: JByteArray,
) {
    intern_write_bytes(&mut env, &mut *writer, content).unwrap_or_else(|e| {
        e.throw(&mut env);
    })
}

fn intern_write_bytes(
    env: &mut JNIEnv,
    writer: &mut BlockingWriter,
    content: JByteArray,
) -> crate::Result<()> {
    let content = env.convert_byte_array(content)?;
    writer.write(content)?;
    Ok(())
}
