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

use jni::Env;
use jni::jni_sig;
use jni::jni_str;
use jni::objects::JObject;
use jni::objects::JValue;
use opendal::Entry;
use opendal::OperatorInfo;
use opendal::raw::PresignedRequest;

mod async_operator;
mod capability;
mod convert;
mod error;
mod executor;
mod layer;
mod metadata;
mod operator;
mod operator_input_stream;
mod operator_output_stream;
mod operator_reader;
mod options;
#[cfg(feature = "test-native-panics")]
mod panic_test;
mod utility;

pub(crate) use metadata::make_metadata;
pub(crate) use options::{
    make_delete_options, make_list_options, make_read_options, make_reader_options,
    make_stat_options, make_write_options,
};

pub(crate) type Result<T> = std::result::Result<T, error::Error>;

fn make_presigned_request<'a>(env: &mut Env<'a>, req: PresignedRequest) -> Result<JObject<'a>> {
    let method = env.new_string(req.method().as_str())?;
    let uri = env.new_string(req.uri().to_string())?;
    let headers = {
        let mut map = HashMap::new();
        for (k, v) in req.header().iter() {
            let key = k.to_string();
            let value = v.to_str().map_err(|err| {
                opendal::Error::new(opendal::ErrorKind::Unexpected, err.to_string())
            })?;
            map.insert(key, value.to_owned());
        }
        map
    };
    let headers = convert::hashmap_to_jmap(env, &headers)?;
    let result = env.new_object(
        jni_str!("org/apache/opendal/PresignedRequest"),
        jni_sig!("(Ljava/lang/String;Ljava/lang/String;Ljava/util/Map;)V"),
        &[
            JValue::Object(&method),
            JValue::Object(&uri),
            JValue::Object(&headers),
        ],
    )?;
    Ok(result)
}

fn make_operator_info<'a>(env: &mut Env<'a>, info: OperatorInfo) -> Result<JObject<'a>> {
    let scheme = env.new_string(info.scheme())?;
    let root = env.new_string(info.root())?;
    let name = env.new_string(info.name())?;
    let capability_obj = capability::make_capability(env, info.capability())?;

    let result = env.new_object(
        jni_str!("org/apache/opendal/OperatorInfo"),
        jni_sig!(
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Lorg/apache/opendal/Capability;)V"
        ),
        &[
            JValue::Object(&scheme),
            JValue::Object(&root),
            JValue::Object(&name),
            JValue::Object(&capability_obj),
        ],
    )?;
    Ok(result)
}

fn make_entry<'a>(env: &mut Env<'a>, entry: Entry) -> Result<JObject<'a>> {
    let path = env.new_string(entry.path())?;
    let metadata = make_metadata(env, entry.metadata().to_owned())?;

    Ok(env.new_object(
        jni_str!("org/apache/opendal/Entry"),
        jni_sig!("(Ljava/lang/String;Lorg/apache/opendal/Metadata;)V"),
        &[JValue::Object(&path), JValue::Object(&metadata)],
    )?)
}
