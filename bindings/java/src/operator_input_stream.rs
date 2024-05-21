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

use crate::convert::jstring_to_string;
use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::sys::{jbyteArray, jlong};
use jni::JNIEnv;
use opendal::{BlockingOperator, StdBytesIterator};

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorInputStream_constructReader(
    mut env: JNIEnv,
    _: JClass,
    op: *mut BlockingOperator,
    path: JString,
) -> jlong {
    intern_construct_reader(&mut env, &mut *op, path).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_construct_reader(
    env: &mut JNIEnv,
    op: &mut BlockingOperator,
    path: JString,
) -> crate::Result<jlong> {
    let path = jstring_to_string(env, &path)?;
    let reader = op.reader(&path)?.into_bytes_iterator(..)?;
    Ok(Box::into_raw(Box::new(reader)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorInputStream_disposeReader(
    _: JNIEnv,
    _: JClass,
    reader: *mut StdBytesIterator,
) {
    drop(Box::from_raw(reader));
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorInputStream_readNextBytes(
    mut env: JNIEnv,
    _: JClass,
    reader: *mut StdBytesIterator,
) -> jbyteArray {
    intern_read_next_bytes(&mut env, &mut *reader).unwrap_or_else(|e| {
        e.throw(&mut env);
        JByteArray::default().into_raw()
    })
}

fn intern_read_next_bytes(
    env: &mut JNIEnv,
    reader: &mut StdBytesIterator,
) -> crate::Result<jbyteArray> {
    match reader
        .next()
        .transpose()
        .map_err(|err| opendal::Error::new(opendal::ErrorKind::Unexpected, &err.to_string()))?
    {
        None => Ok(JObject::null().into_raw()),
        Some(content) => {
            let result = env.byte_array_from_slice(&content)?;
            Ok(result.into_raw())
        }
    }
}
