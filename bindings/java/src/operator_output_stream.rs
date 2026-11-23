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

use jni::Env;
use jni::EnvUnowned;
use jni::objects::JByteArray;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JString;
use jni::sys::jint;
use jni::sys::jlong;
use opendal::blocking;

use crate::convert::jstring_to_string;
use crate::error::ThrowException;

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorOutputStream_constructWriter<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *const blocking::Operator,
    path: JString<'local>,
    options: JObject<'local>,
) -> jlong {
    env.with_env(|env| {
        let op_ref = unsafe { &*op };
        intern_construct_write(env, op_ref, path, options)
    })
    .resolve::<ThrowException>()
}

fn intern_construct_write(
    env: &mut Env,
    op: &blocking::Operator,
    path: JString,
    options: JObject,
) -> crate::Result<jlong> {
    use crate::make_write_options;

    let path = jstring_to_string(env, &path)?;
    let options = make_write_options(env, &options)?;
    let writer = op.writer_options(&path, options)?;
    Ok(Box::into_raw(Box::new(writer)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorOutputStream_disposeWriter<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    writer: *mut blocking::Writer,
) {
    env.with_env(|_env| {
        let mut writer = unsafe { Box::from_raw(writer) };
        intern_dispose_write(&mut writer)
    })
    .resolve::<ThrowException>()
}

fn intern_dispose_write(writer: &mut blocking::Writer) -> crate::Result<()> {
    writer.close()?;
    Ok(())
}

/// # Safety
///
/// `writer` must point to a live writer with exclusive access, and `offset` and
/// `length` must describe a valid range of `content`.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OperatorOutputStream_writeByteRange<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    writer: *mut blocking::Writer,
    content: JByteArray<'local>,
    offset: jint,
    length: jint,
) {
    env.with_env(|env| -> crate::Result<()> {
        let mut owned = vec![0u8; length as usize];
        // SAFETY: i8 and u8 have the same size, alignment, and valid bit patterns.
        let bytes =
            unsafe { std::slice::from_raw_parts_mut(owned.as_mut_ptr().cast(), owned.len()) };
        // Copy into Rust-owned storage before the writer can retain the buffer.
        content.get_region(env, offset, bytes)?;
        let writer = unsafe { &mut *writer };
        writer.write(owned)?;
        Ok(())
    })
    .resolve::<ThrowException>()
}
