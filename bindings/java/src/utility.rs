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
use jni::jni_str;
use jni::objects::IntoAuto;
use jni::objects::JClass;
use jni::objects::JObject;
use jni::objects::JObjectArray;
use jni::sys::jsize;

use crate::Result;
use crate::error::ThrowException;

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_org_apache_opendal_OpenDAL_loadEnabledServices<'local>(
    mut env: EnvUnowned<'local>,
    _: JClass<'local>,
) -> JObjectArray<'local> {
    env.with_env(|env| intern_load_enabled_services(env))
        .resolve::<ThrowException>()
}

fn intern_load_enabled_services<'local>(env: &mut Env<'local>) -> Result<JObjectArray<'local>> {
    opendal::init_default_registry();
    let services = opendal::OperatorRegistry::get().schemes();
    let len = services.len() as jsize;

    let res = env.new_object_array(len, jni_str!("java/lang/String"), JObject::null())?;

    for (idx, service) in services.into_iter().enumerate() {
        let srv = env.new_string(service)?.auto();
        res.set_element(env, idx, &srv)?;
    }

    Ok(res)
}
