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

use jni::EnvUnowned;
use jni::jni_str;
use jni::objects::{JByteArray, JClass, JLongArray, JObject, JObjectArray, JString};
use jni::sys::jlong;
use jni::sys::jsize;
use opendal::blocking;
use std::ops::Bound;

use crate::convert;
use crate::error::ThrowException;

/// # Safety
///
/// `op` must point to a live blocking operator for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_reader<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *const blocking::Operator,
    path: JString<'local>,
    options: JObject<'local>,
) -> jlong {
    env.with_env(|env| -> crate::Result<_> {
        let op = unsafe { &*op };
        let path = convert::jstring_to_string(env, &path)?;
        let options = crate::make_reader_options(env, &options)?;
        let reader = op.reader_options(&path, options)?;
        Ok(Box::into_raw(Box::new(reader)) as jlong)
    })
    .resolve::<ThrowException>()
}

/// # Safety
///
/// `reader` must point to a live blocking reader for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorReader_readBytes<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    reader: *const blocking::Reader,
    offset: jlong,
    length: jlong,
) -> JByteArray<'local> {
    env.with_env(|env| -> crate::Result<_> {
        let reader = unsafe { &*reader };
        let range = convert::offset_length_to_range(offset, length)?;
        let content = reader.read(range)?;
        convert::bytes_to_jbytearray(env, content.to_vec())
    })
    .resolve::<ThrowException>()
}

/// # Safety
///
/// `reader` must point to a live blocking reader for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorReader_createBytesIterator<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    reader: *const blocking::Reader,
    offset: jlong,
    length: jlong,
) -> jlong {
    env.with_env(|_| -> crate::Result<_> {
        let reader = unsafe { &*reader };
        let range = convert::offset_length_to_range(offset, length)?;
        let iter = reader.clone().into_bytes_iterator(range)?;
        Ok(Box::into_raw(Box::new(iter)) as jlong)
    })
    .resolve::<ThrowException>()
}

/// # Safety
///
/// `reader` must point to a live blocking reader for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorReader_fetchRanges<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    reader: *const blocking::Reader,
    offsets: JLongArray<'local>,
    lengths: JLongArray<'local>,
) -> JObjectArray<'local> {
    env.with_env(|env| -> crate::Result<_> {
        let count = offsets.len(env)?;
        if lengths.len(env)? != count {
            return Err(opendal::Error::new(
                opendal::ErrorKind::RangeNotSatisfied,
                "offsets and lengths must have the same size",
            )
            .into());
        }
        let mut starts = vec![0; count];
        let mut sizes = vec![0; count];
        offsets.get_region(env, 0, &mut starts)?;
        lengths.get_region(env, 0, &mut sizes)?;
        let ranges = starts
            .into_iter()
            .zip(sizes)
            .map(
                |(offset, length)| match convert::offset_length_to_range(offset, length)? {
                    (Bound::Included(start), Bound::Excluded(end)) => Ok(start..end),
                    _ => Err(opendal::Error::new(
                        opendal::ErrorKind::RangeNotSatisfied,
                        "fetch requires non-negative lengths",
                    )
                    .into()),
                },
            )
            .collect::<crate::Result<Vec<_>>>()?;
        let reader = unsafe { &*reader };
        let buffers = reader.fetch(ranges)?;
        let output = env.new_object_array(count as jsize, jni_str!("[B"), JObject::null())?;
        for (index, buffer) in buffers.into_iter().enumerate() {
            env.with_local_frame(2, |env| -> crate::Result<()> {
                let bytes = convert::bytes_to_jbytearray(env, buffer.to_vec())?;
                output.set_element(env, index, &bytes)?;
                Ok(())
            })?;
        }
        Ok(output)
    })
    .resolve::<ThrowException>()
}

/// # Safety
///
/// `reader` must be a live handle allocated by `Operator.reader`, with no calls in progress.
/// It must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorReader_disposeReader<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    reader: *mut blocking::Reader,
) {
    unsafe { drop(Box::from_raw(reader)) };
}
