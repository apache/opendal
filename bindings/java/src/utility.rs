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

use jni::objects::{JClass, JObject};
use jni::sys::jobject;
use jni::JNIEnv;
use opendal::Scheme;

use crate::{string_to_jstring, Result};

/// # Safety
///
/// This function should not be called before the Operator are ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_OpenDAL_loadEnabledServices(
    mut env: JNIEnv,
    _: JClass,
) -> jobject {
    intern_load_enabled_services(&mut env).unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::default().into_raw()
    })
}

fn intern_load_enabled_services(env: &mut JNIEnv) -> Result<jobject> {
    let services = Scheme::enabled();

    let list = env.new_object("java/util/ArrayList", "()V", &[])?;
    let jlist = env.get_list(&list)?;

    for service in services {
        let srv = string_to_jstring(env, Some(&service.to_string()))?;
        jlist.add(env, &srv)?;
    }

    Ok(list.into_raw())
}
