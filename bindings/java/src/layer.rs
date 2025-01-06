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

use jni::objects::JClass;
use jni::sys::jboolean;
use jni::sys::jfloat;
use jni::sys::jlong;
use jni::JNIEnv;
use opendal::layers::ConcurrentLimitLayer;
use opendal::layers::RetryLayer;
use opendal::Operator;

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_layer_RetryLayer_doLayer(
    _: JNIEnv,
    _: JClass,
    op: *mut Operator,
    jitter: jboolean,
    factor: jfloat,
    min_delay: jlong,
    max_delay: jlong,
    max_times: jlong,
) -> jlong {
    let op = unsafe { &*op };
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
    Box::into_raw(Box::new(op.clone().layer(retry))) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_layer_ConcurrentLimitLayer_doLayer(
    _: JNIEnv,
    _: JClass,
    op: *mut Operator,
    permits: jlong,
) -> jlong {
    let op = unsafe { &*op };
    let concurrent_limit = ConcurrentLimitLayer::new(permits as usize);
    Box::into_raw(Box::new(op.clone().layer(concurrent_limit))) as jlong
}
