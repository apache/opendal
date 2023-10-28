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

use std::time::Duration;

use jni::objects::{JClass, JObject};
use jni::sys::{jboolean, jfloat, jlong, jobject};
use jni::JNIEnv;

use opendal::layers::RetryLayer;

use crate::operator::{get_operator, make_operator};
use crate::Result;

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_layer_RetryLayer_doLayer<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    op: JObject<'local>,
    jitter: jboolean,
    factor: jfloat,
    min_delay: jlong,
    max_delay: jlong,
    max_times: jlong,
) -> jobject {
    intern_retry_layer(
        &mut env, op, jitter, factor, min_delay, max_delay, max_times,
    )
    .unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::default().into_raw()
    })
}

fn intern_retry_layer<'local>(
    env: &mut JNIEnv<'local>,
    op: JObject<'local>,
    jitter: jboolean,
    factor: jfloat,
    min_delay: jlong,
    max_delay: jlong,
    max_times: jlong,
) -> Result<jobject> {
    let op = get_operator(env, op)?;
    let mut retry = RetryLayer::new();
    retry = retry.with_factor(factor);
    retry = retry.with_min_delay(Duration::from_nanos(min_delay as u64));
    retry = retry.with_max_delay(Duration::from_nanos(max_delay as u64));
    if jitter != 0 {
        retry = retry.with_jitter()
    }
    if max_times >= 0 {
        retry = retry.with_max_times(max_times as usize);
    }
    let op = op.clone().layer(retry);
    Ok(make_operator(env, op)?.into_raw())
}
