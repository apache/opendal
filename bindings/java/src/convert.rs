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

use jni::errors::Result;
use jni::objects::{JMap, JObject, JString, JThrowable, JValue};
use jni::JNIEnv;
use opendal::ErrorKind;
use std::collections::HashMap;

pub(crate) fn error_to_error<E>(error: E) -> opendal::Error
where
    E: Into<anyhow::Error> + ToString,
{
    opendal::Error::new(ErrorKind::Unexpected, &error.to_string()).set_source(error)
}

pub(crate) fn error_to_exception<'local>(
    env: &mut JNIEnv<'local>,
    error: opendal::Error,
) -> Result<JThrowable<'local>> {
    let class = env.find_class("org/apache/opendal/exception/ODException")?;

    let code = env.new_string(match error.kind() {
        ErrorKind::Unexpected => "Unexpected",
        ErrorKind::Unsupported => "Unsupported",
        ErrorKind::ConfigInvalid => "ConfigInvalid",
        ErrorKind::NotFound => "NotFound",
        ErrorKind::PermissionDenied => "PermissionDenied",
        ErrorKind::IsADirectory => "IsADirectory",
        ErrorKind::NotADirectory => "NotADirectory",
        ErrorKind::AlreadyExists => "AlreadyExists",
        ErrorKind::RateLimited => "RateLimited",
        ErrorKind::IsSameFile => "IsSameFile",
        ErrorKind::ConditionNotMatch => "ConditionNotMatch",
        ErrorKind::ContentTruncated => "ContentTruncated",
        ErrorKind::ContentIncomplete => "ContentIncomplete",
        _ => "Unexpected",
    })?;
    let message = env.new_string(error.to_string())?;

    let sig = "(Ljava/lang/String;Ljava/lang/String;)V";
    let params = &[JValue::Object(&code), JValue::Object(&message)];
    env.new_object(class, sig, params).map(JThrowable::from)
}

pub(crate) fn jmap_to_hashmap(
    env: &mut JNIEnv,
    params: &JObject,
) -> Result<HashMap<String, String>> {
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
