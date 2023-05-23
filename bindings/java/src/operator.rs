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

use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jlong;
use jni::JNIEnv;

use opendal::{Operator, Scheme};

use crate::error::Error;
use crate::{get_current_env, Result};
use crate::{jmap_to_hashmap, RUNTIME};

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_Operator_constructor(
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
    let scheme = Scheme::from_str(env.get_string(&scheme)?.to_str()?)?;
    let map = jmap_to_hashmap(env, &map)?;
    let op = Operator::via_map(scheme, map)?;
    Ok(Box::into_raw(Box::new(op)) as jlong)
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_disposeInternal(
    _: JNIEnv,
    _: JClass,
    op: *mut Operator,
) {
    drop(Box::from_raw(op));
}

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_Operator_write(
    mut env: JNIEnv,
    _: JClass,
    op: *mut Operator,
    file: JString,
    content: JString,
) -> jlong {
    intern_write(&mut env, op, file, content).unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

fn intern_write(
    env: &mut JNIEnv,
    op: *mut Operator,
    file: JString,
    content: JString,
) -> Result<jlong> {
    let op = unsafe { &mut *op };
    let file = env.get_string(&file)?.to_str()?.to_string();
    let content = env.get_string(&content)?.to_str()?.to_string();

    let id = request_id(env)?;

    let runtime = unsafe { RUNTIME.get_unchecked() };
    runtime.spawn(async move {
        let result = match op.write(&file, content).await {
            Ok(()) => Ok(JObject::null()),
            Err(err) => Err(Error::from(err)),
        };
        complete_future(id, result)
    });

    Ok(id)
}

fn request_id(env: &mut JNIEnv) -> Result<jlong> {
    let registry = env
        .call_static_method(
            "org/apache/opendal/Operator",
            "registry",
            "()Lorg/apache/opendal/Operator$AsyncRegistry;",
            &[],
        )?
        .l()?;
    Ok(env.call_method(registry, "requestId", "()J", &[])?.j()?)
}

fn complete_future(id: jlong, result: Result<JObject>) {
    let mut env = unsafe { get_current_env() };
    let future = get_future(&mut env, id).unwrap();
    match result {
        Ok(result) => env
            .call_method(
                future,
                "complete",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(&result)],
            )
            .unwrap(),
        Err(err) => {
            let exception = err.to_exception(&mut env).unwrap();
            env.call_method(
                future,
                "completeExceptionally",
                "(Ljava/lang/Throwable;)Z",
                &[JValue::Object(&exception)],
            )
            .unwrap()
        }
    };
}

fn get_future<'local>(env: &mut JNIEnv<'local>, id: jlong) -> Result<JObject<'local>> {
    let registry = env
        .call_static_method(
            "org/apache/opendal/Operator",
            "registry",
            "()Lorg/apache/opendal/Operator$AsyncRegistry;",
            &[],
        )?
        .l()?;
    Ok(env
        .call_method(
            registry,
            "get",
            "(J)Ljava/util/concurrent/CompletableFuture;",
            &[JValue::Long(id)],
        )?
        .l()?)
}
