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

use std::collections::HashMap;

use jni::objects::JMap;
use jni::objects::JObject;
use jni::objects::JString;
use jni::sys::jlong;
use jni::JNIEnv;

pub(crate) fn usize_to_jlong(n: Option<usize>) -> jlong {
    // usize is always >= 0, so we can use -1 to identify the empty value.
    n.map_or(-1, |v| v as jlong)
}

pub(crate) fn jmap_to_hashmap(
    env: &mut JNIEnv,
    params: &JObject,
) -> crate::Result<HashMap<String, String>> {
    let map = JMap::from_env(env, params)?;
    let mut iter = map.iter(env)?;

    let mut result: HashMap<String, String> = HashMap::new();
    while let Some(e) = iter.next(env)? {
        let k = JString::from(e.0);
        let v = JString::from(e.1);
        result.insert(env.get_string(&k)?.into(), env.get_string(&v)?.into());
    }

    Ok(result)
}

pub(crate) fn hashmap_to_jmap<'a>(
    env: &mut JNIEnv<'a>,
    map: &HashMap<String, String>,
) -> crate::Result<JObject<'a>> {
    let map_object = env.new_object("java/util/HashMap", "()V", &[])?;
    let jmap = env.get_map(&map_object)?;
    for (k, v) in map {
        let key = env.new_string(k)?;
        let value = env.new_string(v)?;
        jmap.put(env, &key, &value)?;
    }
    Ok(map_object)
}

pub(crate) fn string_to_jstring<'a>(
    env: &mut JNIEnv<'a>,
    s: Option<&str>,
) -> crate::Result<JObject<'a>> {
    s.map_or_else(
        || Ok(JObject::null()),
        |v| Ok(env.new_string(v.to_string())?.into()),
    )
}

pub(crate) fn get_optional_string_from_object<'a>(
    env: &mut JNIEnv<'a>,
    obj: &JObject,
    method: &str,
) -> crate::Result<Option<String>> {
    let result = env
        .call_method(obj, method, "()Ljava/lang/String;", &[])?
        .l()?;
    if result.is_null() {
        Ok(None)
    } else {
        Ok(Some(jstring_to_string(env, &JString::from(result))?))
    }
}

pub(crate) fn get_optional_map_from_object<'a>(
    env: &mut JNIEnv<'a>,
    obj: &JObject,
    method: &str,
) -> crate::Result<Option<HashMap<String, String>>> {
    let result = env
        .call_method(obj, method, "()Ljava/util/Map;", &[])?
        .l()?;
    if result.is_null() {
        Ok(None)
    } else {
        Ok(Some(jmap_to_hashmap(env, &result)?))
    }
}

/// # Safety
///
/// The caller must guarantee that the Object passed in is an instance
/// of `java.lang.String`, passing in anything else will lead to undefined behavior.
pub(crate) fn jstring_to_string(env: &mut JNIEnv, s: &JString) -> crate::Result<String> {
    let res = unsafe { env.get_string_unchecked(s)? };
    Ok(res.into())
}
