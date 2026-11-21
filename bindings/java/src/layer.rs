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

use crate::Result;
use crate::convert::jstring_to_string;
use crate::error::ThrowException;
use jni::Env;
use jni::EnvUnowned;
use jni::objects::JClass;
use jni::objects::JString;
use jni::sys::jboolean;
use jni::sys::jfloat;
use jni::sys::jlong;
use opendal::Operator;
use opendal::layers::CapabilityOverrideLayer;
use opendal::layers::ConcurrentLimitLayer;
use opendal::layers::MimeGuessLayer;
use opendal::layers::RetryLayer;
use opendal::layers::ThrottleLayer;
use opendal::layers::TimeoutLayer;
use opendal::{Error, ErrorKind};

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_RetryLayer_doLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
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
    if jitter {
        retry = retry.with_jitter()
    }
    if max_times >= 0 {
        retry = retry.with_max_times(max_times as usize);
    }
    Box::into_raw(Box::new(op.clone().layer(retry))) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_CapabilityOverrideLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    overrides: JString<'local>,
) -> jlong {
    env.with_env(|env| intern_capability_override_layer(env, op, overrides))
        .resolve::<ThrowException>()
}

fn intern_capability_override_layer(
    env: &mut Env,
    op: *mut Operator,
    overrides: JString,
) -> Result<jlong> {
    let op = unsafe { &*op };
    let overrides = jstring_to_string(env, &overrides)?;
    let layer = CapabilityOverrideLayer::from_overrides(&overrides)?;
    Ok(Box::into_raw(Box::new(op.clone().layer(layer))) as jlong)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_ConcurrentLimitLayer_doLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    permits: jlong,
) -> jlong {
    let op = unsafe { &*op };
    let concurrent_limit = ConcurrentLimitLayer::new(permits as usize);
    Box::into_raw(Box::new(op.clone().layer(concurrent_limit))) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_MimeGuessLayer_doLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
) -> jlong {
    let op = unsafe { &*op };
    Box::into_raw(Box::new(op.clone().layer(MimeGuessLayer::new()))) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_ThrottleLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    bandwidth: jlong,
    burst: jlong,
) -> jlong {
    env.with_env(|_| -> Result<jlong> {
        let Ok(bandwidth @ 1..) = u32::try_from(bandwidth) else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "bandwidth must be between 1 and 4294967295",
            )
            .into());
        };
        let Ok(burst @ 1..) = u32::try_from(burst) else {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "burst must be between 1 and 4294967295",
            )
            .into());
        };
        let op = unsafe { &*op };
        let throttle = ThrottleLayer::new(bandwidth, burst);
        Ok(Box::into_raw(Box::new(op.clone().layer(throttle))) as jlong)
    })
    .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_TimeoutLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    timeout_nanos: jlong,
    io_timeout_nanos: jlong,
) -> jlong {
    env.with_env(|_| -> Result<jlong> {
        let timeout_nanos = u64::try_from(timeout_nanos)
            .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "timeout must be non-negative"))?;
        let io_timeout_nanos = u64::try_from(io_timeout_nanos)
            .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "ioTimeout must be non-negative"))?;
        let op = unsafe { &*op };
        let timeout = TimeoutLayer::new()
            .with_timeout(Duration::from_nanos(timeout_nanos))
            .with_io_timeout(Duration::from_nanos(io_timeout_nanos));
        Ok(Box::into_raw(Box::new(op.clone().layer(timeout))) as jlong)
    })
    .resolve::<ThrowException>()
}
