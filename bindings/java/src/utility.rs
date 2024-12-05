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

use jni::objects::JClass;
use jni::objects::JObject;
use jni::sys::jobjectArray;
use jni::sys::jsize;
use jni::JNIEnv;
use opendal::Scheme;

use crate::convert::string_to_jstring;
use crate::Result;

/// # Safety
///
/// This function should not be called before the Operator is ready.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_opendal_OpenDAL_loadEnabledServices(
    mut env: JNIEnv,
    _: JClass,
) -> jobjectArray {
    intern_load_enabled_services(&mut env).unwrap_or_else(|e| {
        e.throw(&mut env);
        JObject::default().into_raw()
    })
}

fn intern_load_enabled_services(env: &mut JNIEnv) -> Result<jobjectArray> {
    let services = Scheme::enabled();
    let res = env.new_object_array(services.len() as jsize, "java/lang/String", JObject::null())?;

    for (idx, service) in services.iter().enumerate() {
        let srv = string_to_jstring(env, Some(&service.to_string()))?;
        env.set_object_array_element(&res, idx as jsize, srv)?;
    }

    Ok(res.into_raw())
}
