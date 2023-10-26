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
use jni::sys::{jboolean, jfloat, jlong};
use jni::JNIEnv;

use opendal::layers::RetryLayer;
use opendal::raw::{FusedAccessor, Layer};

pub(crate) enum NativeLayer {
    Retry(RetryLayer),
}

impl NativeLayer {
    pub(crate) fn into_inner(self) -> impl Layer<FusedAccessor> {
        match self {
            NativeLayer::Retry(layer) => layer,
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_layer_RetryNativeLayerSpec_makeNativeLayer(
    _: JNIEnv,
    _: JClass,
    jitter: jboolean,
    factor: jfloat,
    min_delay: jlong,
    max_delay: jlong,
    max_times: jlong,
) -> jlong {
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
    Box::into_raw(Box::new(NativeLayer::Retry(retry))) as jlong
}
