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
use jni::sys::{jlong, jobject};
use jni::JNIEnv;

use opendal::Result;
use opendal::{Operator, Scheme};

use crate::{convert, or_throw, ENV, RUNTIME};

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_Operator_constructor(
    mut env: JNIEnv,
    _: JClass,
    scheme: JString,
    map: JObject,
) -> jlong {
    let res = intern_constructor(&mut env, scheme, map);
    or_throw(&mut env, res)
}

fn intern_constructor(env: &mut JNIEnv, scheme: JString, map: JObject) -> Result<jlong> {
    let scheme = {
        let res = env.get_string(&scheme).map_err(convert::error_to_error)?;
        let res = res.to_str().map_err(convert::error_to_error)?;
        Scheme::from_str(res)?
    };
    let map = convert::jmap_to_hashmap(env, &map).map_err(convert::error_to_error)?;
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
) -> jobject {
    let op = &mut *op;

    let file: String = env.get_string(&file).unwrap().into();
    let content: String = env.get_string(&content).unwrap().into();

    let class = "java/util/concurrent/CompletableFuture";
    let f = env.new_object(class, "()V", &[]).unwrap();

    // keep the future alive, so that we can complete it later
    // but this approach will be limited by global ref table size (65535)
    let future = env.new_global_ref(&f).unwrap();

    RUNTIME.get_unchecked().spawn(async move {
        let result = op.write(&file, content).await;

        let env = ENV.with(|cell| *cell.borrow_mut()).unwrap();
        let mut env = JNIEnv::from_raw(env).unwrap();

        match result {
            Ok(()) => env
                .call_method(
                    future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(&JObject::null())],
                )
                .unwrap(),
            Err(err) => {
                let exception = convert::error_to_exception(&mut env, err).unwrap();
                env.call_method(
                    future,
                    "completeExceptionally",
                    "(Ljava/lang/Throwable;)Z",
                    &[JValue::Object(&exception)],
                )
                .unwrap()
            }
        }
    });

    f.as_raw()
}
