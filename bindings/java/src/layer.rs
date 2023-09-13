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

use jni::objects::{JClass, JList, JObject};
use jni::sys::{jboolean, jfloat, jint, jlong};
use jni::JNIEnv;
use std::time::Duration;

use opendal::layers::{BlockingLayer, RetryLayer};

use crate::{get_global_runtime, Result};

pub(crate) enum LayerBridge {
    Retry(RetryLayer),
    Blocking(BlockingLayer),
}

pub(crate) fn construct_layers(
    env: &mut JNIEnv,
    layers: &JObject,
) -> Result<Vec<Box<LayerBridge>>> {
    let layers = JList::from_env(env, layers)?;
    let mut results = vec![];
    let mut iterator = layers.iter(env)?;
    while let Some(layer) = iterator.next(env)? {
        let layer = construct_layer(env, layer)?;
        results.push(layer);
    }
    Ok(results)
}

fn construct_layer(env: &mut JNIEnv, layer: JObject) -> Result<Box<LayerBridge>> {
    let ptr = env.call_method(layer, "constructLayer", "()J", &[])?.j()? as *mut LayerBridge;
    Ok(unsafe { Box::from_raw(ptr) })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_BlockingLayerSpec_constructLayer0(
    mut env: JNIEnv,
    _: JClass,
) -> jlong {
    fn intern_construct_layer() -> Result<jlong> {
        let _guard = unsafe { get_global_runtime() }.enter();
        let layer = BlockingLayer::create()?;
        Ok(Box::into_raw(Box::new(LayerBridge::Blocking(layer))) as jlong)
    }

    intern_construct_layer().unwrap_or_else(|e| {
        e.throw(&mut env);
        0
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_opendal_RetryLayerSpec_constructLayer0(
    _: JNIEnv,
    _: JClass,
    max_times: jint,
    factor: jfloat,
    jitter: jboolean,
    max_delay: jlong,
    min_delay: jlong,
) -> jlong {
    let mut layer = RetryLayer::new();
    if max_times >= 0 {
        layer = layer.with_max_times(max_times as usize);
    }
    if factor >= 1.0 {
        layer = layer.with_factor(factor);
    }
    if jitter != 0 {
        layer = layer.with_jitter();
    }
    if max_delay >= 0 {
        layer = layer.with_max_delay(Duration::from_nanos(max_delay as u64));
    }
    if min_delay >= 0 {
        layer = layer.with_min_delay(Duration::from_nanos(min_delay as u64));
    }
    Box::into_raw(Box::new(LayerBridge::Retry(layer))) as jlong
}
