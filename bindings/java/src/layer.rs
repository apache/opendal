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
use crate::convert::{jstring_to_string, read_int64_field};
use crate::error::ThrowException;
use jni::Env;
use jni::EnvUnowned;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::IntoAuto;
use jni::objects::JClass;
use jni::objects::JLongArray;
use jni::objects::JObject;
use jni::objects::JObjectArray;
use jni::objects::JString;
use jni::sys::jboolean;
use jni::sys::jdouble;
use jni::sys::jfloat;
use jni::sys::jint;
use jni::sys::jlong;
use opendal::Operator;
use opendal::blocking;
use opendal::layers::CacheLayer;
use opendal::layers::CapabilityCheckLayer;
use opendal::layers::CapabilityOverrideLayer;
use opendal::layers::ChaosLayer;
use opendal::layers::ConcurrentLimitLayer;
use opendal::layers::HedgeLayer;
use opendal::layers::ImmutableIndexLayer;
use opendal::layers::LoggingLayer;
use opendal::layers::MimeGuessLayer;
use opendal::layers::RetryLayer;
use opendal::layers::RouteLayer;
use opendal::layers::RouteLayerBuilder;
use opendal::layers::SanityCheckLayer;
use opendal::layers::TailCutLayer;
use opendal::layers::ThrottleLayer;
use opendal::layers::TimeoutLayer;
use opendal::raw::Operation;
use opendal::{Error, ErrorKind};

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_RetryLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    jitter: jboolean,
    factor: jfloat,
    min_delay: jlong,
    max_delay: jlong,
    max_times: jlong,
) -> jlong {
    env.with_env(|_| -> Result<jlong> {
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
        Ok(Box::into_raw(Box::new(op.clone().layer(retry))) as jlong)
    })
    .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_CacheLayer_createLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    cache: JObject<'local>,
    is_blocking: jboolean,
    chunk_size: jlong,
    ttl_seconds: jlong,
    ttl_nanos: jint,
) -> jlong {
    env.with_env(|env| -> Result<jlong> {
        let chunk_size = usize::try_from(chunk_size).map_err(|_| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "chunkSize exceeds the native size range",
            )
        })?;
        let handle = read_int64_field(env, &cache, "nativeHandle")?;
        // Java holds the cache operator's monitor and checks that it is open.
        let cache = if is_blocking {
            unsafe { &*(handle as *const blocking::Operator) }
                .clone()
                .into()
        } else {
            unsafe { &*(handle as *const Operator) }.clone()
        };
        let mut layer = CacheLayer::new(cache)?;
        if chunk_size != 0 {
            layer = layer.with_chunk_size(chunk_size);
        }
        if let Ok(seconds) = u64::try_from(ttl_seconds) {
            layer = layer.with_ttl(Duration::new(seconds, ttl_nanos as u32));
        }
        Ok(Box::into_raw(Box::new(layer)) as jlong)
    })
    .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_CacheLayer_doLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *const Operator,
    layer: *const CacheLayer,
) -> jlong {
    let op = unsafe { &*op };
    let layer = unsafe { &*layer };
    Box::into_raw(Box::new(op.clone().layer(layer.clone()))) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_CacheLayer_disposeLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    layer: *mut CacheLayer,
) {
    drop(unsafe { Box::from_raw(layer) });
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
pub extern "system" fn Java_org_apache_opendal_layer_ConcurrentLimitLayer_doLayerWithLimits<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    permits: jlong,
    read_permits: jlong,
    write_permits: jlong,
    list_permits: jlong,
    http_permits: jlong,
) -> jlong {
    env.with_env(|env| -> Result<jlong> {
        let [
            Ok(permits @ 1..),
            Ok(read_permits),
            Ok(write_permits),
            Ok(list_permits),
            Ok(http_permits),
        ] = [
            permits,
            read_permits,
            write_permits,
            list_permits,
            http_permits,
        ]
        .map(usize::try_from)
        else {
            env.throw_new(
                jni_str!("java/lang/IllegalArgumentException"),
                jni_str!("permits must be positive when set and fit in the native permit range"),
            )?;
            return Ok(0);
        };

        let mut layer = ConcurrentLimitLayer::new(permits);
        if read_permits != 0 {
            layer = layer.with_read_concurrent_limit(read_permits);
        }
        if write_permits != 0 {
            layer = layer.with_write_concurrent_limit(write_permits);
        }
        if list_permits != 0 {
            layer = layer.with_list_concurrent_limit(list_permits);
        }
        if http_permits != 0 {
            layer = layer.with_http_concurrent_limit(http_permits);
        }

        let op = unsafe { &*op };
        Ok(Box::into_raw(Box::new(op.clone().layer(layer))) as jlong)
    })
    .resolve::<ThrowException>()
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
pub extern "system" fn Java_org_apache_opendal_layer_LoggingLayer_doLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
) -> jlong {
    let op = unsafe { &*op };
    Box::into_raw(Box::new(op.clone().layer(LoggingLayer::default()))) as jlong
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
pub extern "system" fn Java_org_apache_opendal_layer_RouteLayer_createBuilder<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
) -> jlong {
    Box::into_raw(Box::new(RouteLayer::builder())) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_RouteLayer_addRoute<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    builder: *mut RouteLayerBuilder,
    pattern: JString<'local>,
    target: JObject<'local>,
) {
    env.with_env(|env| -> Result<()> {
        let pattern = jstring_to_string(env, &pattern)?;
        let handle = env
            .get_field(&target, jni_str!("nativeHandle"), jni_sig!("J"))?
            .j()? as *const Operator;
        // Java holds the target's monitor and checks that it is open before calling JNI.
        let target = unsafe { &*handle }.clone();
        let builder = unsafe { &mut *builder };
        *builder = std::mem::take(builder).route(pattern, target);
        Ok(())
    })
    .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_RouteLayer_buildLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    builder: *mut RouteLayerBuilder,
) -> jlong {
    env.with_env(|_| -> Result<jlong> {
        let builder = unsafe { &mut *builder };
        let layer = std::mem::take(builder).build()?;
        Ok(Box::into_raw(Box::new(layer)) as jlong)
    })
    .resolve::<ThrowException>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_RouteLayer_disposeBuilder<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    builder: *mut RouteLayerBuilder,
) {
    drop(unsafe { Box::from_raw(builder) });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_RouteLayer_doLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *const Operator,
    layer: *const RouteLayer,
) -> jlong {
    let op = unsafe { &*op };
    let layer = unsafe { &*layer };
    Box::into_raw(Box::new(op.clone().layer(layer.clone()))) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_opendal_layer_RouteLayer_disposeLayer<'local>(
    _: EnvUnowned<'local>,
    _: JClass<'local>,
    layer: *mut RouteLayer,
) {
    drop(unsafe { Box::from_raw(layer) });
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
pub extern "system" fn Java_org_apache_opendal_layer_TailCutLayer_doLayer<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    percentile: jint,
    safety_factor: jdouble,
    window_nanos: jlong,
    min_samples: jint,
    min_deadline_nanos: jlong,
    max_deadline_nanos: jlong,
) -> jlong {
    env.with_env(|env| -> Result<jlong> {
        const MAX_WINDOW_NANOS: jlong = Duration::from_secs(120).as_nanos() as jlong;
        let invalid_argument = if !(50..=99).contains(&percentile) {
            Some(jni_str!("percentile must be between 50 and 99"))
        } else if !(1.0..=5.0).contains(&safety_factor) {
            Some(jni_str!("safetyFactor must be between 1.0 and 5.0"))
        } else if !(0..=MAX_WINDOW_NANOS).contains(&window_nanos) {
            Some(jni_str!("window must be between zero and 120 seconds"))
        } else if min_samples < 0 {
            Some(jni_str!("minSamples must be non-negative"))
        } else if min_deadline_nanos < 0 {
            Some(jni_str!("minDeadline must be non-negative"))
        } else if max_deadline_nanos < 0 {
            Some(jni_str!("maxDeadline must be non-negative"))
        } else if min_deadline_nanos > max_deadline_nanos {
            Some(jni_str!(
                "minDeadline must be less than or equal to maxDeadline"
            ))
        } else {
            None
        };
        if let Some(message) = invalid_argument {
            env.throw_new(jni_str!("java/lang/IllegalArgumentException"), message)?;
            return Ok(0);
        }

        let layer = TailCutLayer::builder()
            .percentile(percentile as u8)
            .safety_factor(safety_factor)
            .window(Duration::from_nanos(window_nanos as u64))
            .min_samples(min_samples as usize)
            .min_deadline(Duration::from_nanos(min_deadline_nanos as u64))
            .max_deadline(Duration::from_nanos(max_deadline_nanos as u64))
            .build();
        let op = unsafe { &*op };
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
pub extern "system" fn Java_org_apache_opendal_layer_TimeoutLayer_doLayerWithTimeouts<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
    op: *mut Operator,
    timeout_nanos: jlong,
    io_timeout_nanos: jlong,
    operations: JObjectArray<'local, JString<'local>>,
    operation_timeout_nanos: JLongArray<'local>,
) -> jlong {
    env.with_env(|env| -> Result<jlong> {
        let timeout_nanos = u64::try_from(timeout_nanos)
            .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "timeout must be non-negative"))?;
        let io_timeout_nanos = u64::try_from(io_timeout_nanos)
            .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "ioTimeout must be non-negative"))?;
        let mut layer = TimeoutLayer::new()
            .with_timeout(Duration::from_nanos(timeout_nanos))
            .with_io_timeout(Duration::from_nanos(io_timeout_nanos));

        let operation_count = operations.len(env)?;
        if operation_count != operation_timeout_nanos.len(env)? {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "operations and operation timeouts must have the same length",
            )
            .into());
        }
        let mut timeouts = vec![0; operation_count];
        operation_timeout_nanos.get_region(env, 0, &mut timeouts)?;
        for (index, timeout) in timeouts.into_iter().enumerate() {
            let name = operations.get_element(env, index)?.auto();
            let name = jstring_to_string(env, &name)?;
            let operation = match name.as_str() {
                "create_dir" => Operation::CreateDir,
                "read" => Operation::Read,
                "write" => Operation::Write,
                "copy" => Operation::Copy,
                "compose" => Operation::Compose,
                "rename" => Operation::Rename,
                "restore" => Operation::Restore,
                "stat" => Operation::Stat,
                "delete" => Operation::Delete,
                "list" => Operation::List,
                "presign" => Operation::Presign,
                _ => {
                    return Err(
                        Error::new(ErrorKind::ConfigInvalid, "unknown timeout operation")
                            .with_context("operation", name)
                            .into(),
                    );
                }
            };
            let timeout = u64::try_from(timeout).map_err(|_| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "operation timeout must be non-negative",
                )
                .with_context("operation", operation.into_static())
            })?;
            layer = layer.with_operation_timeout(operation, Duration::from_nanos(timeout));
        }

        let op = unsafe { &*op };
        Ok(Box::into_raw(Box::new(op.clone().layer(layer))) as jlong)
    })
    .resolve::<ThrowException>()
}
