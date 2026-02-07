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

use jni::JNIEnv;
use jni::objects::JByteArray;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JString;
use jni::sys::jbyteArray;
use jni::sys::jlong;
use opendal::blocking;
use opendal::blocking::StdBytesIterator;

use crate::convert::jstring_to_string;

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorInputStream_constructReader(
    mut env: JNIEnv,
    _: JClass,
    op: *mut blocking::Operator,
    path: JString,
    options: JObject,
) -> jlong {
    let op_ref = unsafe { &mut *op };
    intern_construct_reader(&mut env, op_ref, path, options).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_construct_reader(
    env: &mut JNIEnv,
    op: &mut blocking::Operator,
    path: JString,
    options: JObject,
) -> crate::Result<jlong> {
    use crate::convert;
    use crate::make_reader_options;

    let path = jstring_to_string(env, &path)?;
    let reader_options = make_reader_options(env, &options)?;

    let offset = convert::read_int64_field(env, &options, "offset")?;
    let length = convert::read_int64_field(env, &options, "length")?;
    let range = convert::offset_length_to_range(offset, length)?;

    let reader = op
        .reader_options(&path, reader_options)?
        .into_bytes_iterator(range)?;
    Ok(Box::into_raw(Box::new(reader)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorInputStream_disposeReader(
    _: JNIEnv,
    _: JClass,
    reader: *mut StdBytesIterator,
) {
    unsafe {
        drop(Box::from_raw(reader));
    }
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorInputStream_readNextBytes(
    mut env: JNIEnv,
    _: JClass,
    reader: *mut StdBytesIterator,
) -> jbyteArray {
    let reader_ref = unsafe { &mut *reader };
    intern_read_next_bytes(&mut env, reader_ref).unwrap_or_else(|e| {
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
        .map_err(|err| opendal::Error::new(opendal::ErrorKind::Unexpected, err.to_string()))?
    {
        None => Ok(JObject::null().into_raw()),
        Some(content) => {
            let result = env.byte_array_from_slice(&content)?;
            Ok(result.into_raw())
        }
    }
}
