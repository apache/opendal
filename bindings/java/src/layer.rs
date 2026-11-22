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
use jni::jni_str;
use jni::objects::IntoAuto;
use jni::objects::JClass;
use jni::objects::JObjectArray;
use jni::objects::JString;
use jni::sys::jboolean;
use jni::sys::jdouble;
use jni::sys::jfloat;
use jni::sys::jint;
use jni::sys::jlong;
use opendal::Operator;
use opendal::layers::CapabilityCheckLayer;
use opendal::layers::CapabilityOverrideLayer;
use opendal::layers::ChaosLayer;
use opendal::layers::ConcurrentLimitLayer;
use opendal::layers::HedgeLayer;
use opendal::layers::ImmutableIndexLayer;
use opendal::layers::MimeGuessLayer;
use opendal::layers::RetryLayer;
use opendal::layers::SanityCheckLayer;
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
pub extern "system" fn Java_org_apache_opendal_layer_CapabilityCheckLayer_doLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
) -> jlong {
    let op = unsafe { &*op };
    Box::into_raw(Box::new(op.clone().layer(CapabilityCheckLayer::new()))) as jlong
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
pub extern "system" fn Java_org_apache_opendal_layer_ChaosLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    error_ratio: jdouble,
) -> jlong {
    env.with_env(|env| -> Result<jlong> {
        if !(0.0..=1.0).contains(&error_ratio) {
            env.throw_new(
                jni_str!("java/lang/IllegalArgumentException"),
                jni_str!("errorRatio must be between 0.0 and 1.0"),
            )?;
            return Ok(0);
        }
        let op = unsafe { &*op };
        Ok(Box::into_raw(Box::new(op.clone().layer(ChaosLayer::new(error_ratio)))) as jlong)
    })
    .resolve::<ThrowException>()
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
pub extern "system" fn Java_org_apache_opendal_layer_HedgeLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    stat_delay_nanos: jlong,
    read_delay_nanos: jlong,
    max_hedges: jint,
    hedge_stat: jboolean,
    hedge_read: jboolean,
) -> jlong {
    env.with_env(|env| -> Result<jlong> {
        let (Ok(stat_delay_nanos), Ok(read_delay_nanos)) = (
            u64::try_from(stat_delay_nanos),
            u64::try_from(read_delay_nanos),
        ) else {
            env.throw_new(
                jni_str!("java/lang/IllegalArgumentException"),
                jni_str!("statDelay and readDelay must be non-negative"),
            )?;
            return Ok(0);
        };
        let Ok(max_hedges) = usize::try_from(max_hedges) else {
            env.throw_new(
                jni_str!("java/lang/IllegalArgumentException"),
                jni_str!("maxHedges must be non-negative"),
            )?;
            return Ok(0);
        };
        let mut layer = HedgeLayer::new()
            .with_stat_delay(Duration::from_nanos(stat_delay_nanos))
            .with_read_delay(Duration::from_nanos(read_delay_nanos))
            .with_max_hedges(max_hedges);
        if !hedge_stat {
            layer = layer.without_stat_hedge();
        }
        if !hedge_read {
            layer = layer.without_read_hedge();
        }
        let op = unsafe { &*op };
        Ok(Box::into_raw(Box::new(op.clone().layer(layer))) as jlong)
    })
    .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_ImmutableIndexLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    keys: JObjectArray<'local, JString<'local>>,
) -> jlong {
    env.with_env(|env| -> Result<jlong> {
        let mut layer = ImmutableIndexLayer::new();
        for index in 0..keys.len(env)? {
            let key = keys.get_element(env, index)?.auto();
            layer.insert(jstring_to_string(env, &key)?);
        }
        let op = unsafe { &*op };
        Ok(Box::into_raw(Box::new(op.clone().layer(layer))) as jlong)
    })
    .resolve::<ThrowException>()
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
pub extern "system" fn Java_org_apache_opendal_layer_SanityCheckLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
) -> jlong {
    env.with_env(|_| -> Result<jlong> {
        let op = unsafe { &*op };
        let layer: SanityCheckLayer = Default::default();
        Ok(Box::into_raw(Box::new(op.clone().layer(layer))) as jlong)
    })
    .resolve::<ThrowException>()
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
