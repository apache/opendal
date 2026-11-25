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
use jni::sys::jint;
use jni::sys::jlong;
use opendal::Entry;
use opendal::EntryMode;
use opendal::Metadata;
use opendal::MetadataBuilder;
use opendal::OperatorInfo;
use opendal::raw::PresignedRequest;
use opendal::{Error, ErrorKind};

mod async_operator;
mod capability;
mod convert;
mod error;
mod executor;
mod layer;
mod operator;
mod operator_input_stream;
mod operator_output_stream;
mod operator_reader;
#[cfg(feature = "test-native-panics")]
mod panic_test;
mod utility;

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

fn make_metadata<'a>(env: &mut Env<'a>, metadata: Metadata) -> Result<JObject<'a>> {
    let mode = match metadata.mode() {
        EntryMode::FILE => 0,
        EntryMode::DIR => 1,
        EntryMode::Unknown => 2,
    };

    let last_modified = metadata.last_modified().map_or_else(
        || Ok::<JObject<'_>, error::Error>(JObject::null()),
        |v| {
            Ok(env
                .call_static_method(
                    jni_str!("java/time/Instant"),
                    jni_str!("ofEpochSecond"),
                    jni_sig!("(JJ)Ljava/time/Instant;"),
                    &[
                        JValue::Long(v.into_inner().as_second()),
                        JValue::Long(v.into_inner().subsec_nanosecond() as jlong),
                    ],
                )?
                .l()?)
        },
    )?;

    let cache_control = convert::string_to_jstring(env, metadata.cache_control())?;

    let content_disposition = convert::string_to_jstring(env, metadata.content_disposition())?;

    let content_md5 = convert::string_to_jstring(env, metadata.content_md5())?;

    let content_type = convert::string_to_jstring(env, metadata.content_type())?;

    let etag = convert::string_to_jstring(env, metadata.etag())?;

    let version = convert::string_to_jstring(env, metadata.version())?;

    let content_length = metadata.content_length() as jlong;

    let user_metadata =
        convert::hashmap_to_jmap(env, metadata.user_metadata().into_iter().flatten())?;

    let result = env
        .new_object(
            jni_str!("org/apache/opendal/Metadata"),
            jni_sig!(
                "(IJLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/time/Instant;Ljava/lang/String;Ljava/util/Map;)V"
            ),
            &[
                JValue::Int(mode as jint),
                JValue::Long(content_length),
                JValue::Object(&content_disposition),
                JValue::Object(&content_md5),
                JValue::Object(&content_type),
                JValue::Object(&cache_control),
                JValue::Object(&etag),
                JValue::Object(&last_modified),
                JValue::Object(&version),
                JValue::Object(&user_metadata),
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

fn make_write_options<'a>(
    env: &mut Env<'a>,
    options: &JObject,
) -> Result<opendal::options::WriteOptions> {
    let concurrent = match convert::read_int_field(env, options, "concurrent")? {
        v if v > 0 => v as usize,
        v => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Concurrent must be positive, instead got: {v}"),
            )
            .into());
        }
    };
    Ok(opendal::options::WriteOptions {
        append: convert::read_bool_field(env, options, "append").unwrap_or_default(),
        content_type: convert::read_string_field(env, options, "contentType")?,
        content_disposition: convert::read_string_field(env, options, "contentDisposition")?,
        content_encoding: convert::read_string_field(env, options, "contentEncoding")?,
        cache_control: convert::read_string_field(env, options, "cacheControl")?,
        if_match: convert::read_string_field(env, options, "ifMatch")?,
        if_none_match: convert::read_string_field(env, options, "ifNoneMatch")?,
        if_not_exists: convert::read_bool_field(env, options, "ifNotExists").unwrap_or_default(),
        user_metadata: convert::read_map_field(env, options, "userMetadata")?,
        concurrent,
        chunk: convert::read_jlong_field_to_usize(env, options, "chunk")?,
        ..Default::default()
    })
}

fn make_delete_options(
    env: &mut Env,
    options: &JObject,
) -> Result<opendal::options::DeleteOptions> {
    let metadata = env
        .get_field(
            options,
            jni_str!("ifNotChanged"),
            jni_sig!("Lorg/apache/opendal/Metadata;"),
        )?
        .l()?;
    let if_not_changed = if metadata.is_null() {
        None
    } else {
        // Delete conditions use only the version and ETag from the snapshot.
        let mut builder = MetadataBuilder::unknown();
        if let Some(version) = convert::read_string_field(env, &metadata, "version")? {
            builder.version(version);
        }
        if let Some(etag) = convert::read_string_field(env, &metadata, "etag")? {
            builder.etag(etag);
        }
        Some(builder.build())
    };

    Ok(opendal::options::DeleteOptions {
        version: convert::read_string_field(env, options, "version")?,
        recursive: convert::read_bool_field(env, options, "recursive")?,
        if_match: convert::read_string_field(env, options, "ifMatch")?,
        if_none_match: convert::read_string_field(env, options, "ifNoneMatch")?,
        if_version_match: convert::read_string_field(env, options, "ifVersionMatch")?,
        if_version_not_match: convert::read_string_field(env, options, "ifVersionNotMatch")?,
        if_not_changed,
    })
}

fn make_list_options<'a>(
    env: &mut Env<'a>,
    options: &JObject,
) -> Result<opendal::options::ListOptions> {
    Ok(opendal::options::ListOptions {
        limit: convert::read_jlong_field_to_usize(env, options, "limit")?,
        start_after: convert::read_string_field(env, options, "startAfter")?,
        recursive: convert::read_bool_field(env, options, "recursive").unwrap_or_default(),
        versions: convert::read_bool_field(env, options, "versions").unwrap_or_default(),
        deleted: convert::read_bool_field(env, options, "deleted").unwrap_or_default(),
        ..Default::default()
    })
}

fn make_stat_options(env: &mut Env, options: &JObject) -> Result<opendal::options::StatOptions> {
    Ok(opendal::options::StatOptions {
        if_match: convert::read_string_field(env, options, "ifMatch")?,
        if_none_match: convert::read_string_field(env, options, "ifNoneMatch")?,
        if_version_match: convert::read_string_field(env, options, "ifVersionMatch")?,
        if_version_not_match: convert::read_string_field(env, options, "ifVersionNotMatch")?,
        if_modified_since: convert::read_instant_field_to_timestamp(
            env,
            options,
            "ifModifiedSince",
        )?,
        if_unmodified_since: convert::read_instant_field_to_timestamp(
            env,
            options,
            "ifUnmodifiedSince",
        )?,
        version: convert::read_string_field(env, options, "version")?,
        override_content_type: convert::read_string_field(env, options, "overrideContentType")?,
        override_cache_control: convert::read_string_field(env, options, "overrideCacheControl")?,
        override_content_disposition: convert::read_string_field(
            env,
            options,
            "overrideContentDisposition",
        )?,
    })
}

fn validate_read_execution_options(
    concurrent: i32,
    chunk: i64,
    content_length_hint: i64,
) -> Result<(usize, Option<usize>, Option<u64>)> {
    if concurrent <= 0 {
        return Err(Error::new(ErrorKind::ConfigInvalid, "concurrent must be positive").into());
    }
    let concurrent = usize::try_from(concurrent)
        .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "concurrent is too large"))?;
    let chunk = match chunk {
        -1 => None,
        value if value > 0 => Some(
            usize::try_from(value)
                .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "chunk is too large"))?,
        ),
        _ => {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "chunk must be -1 or positive").into(),
            );
        }
    };
    let content_length_hint = match content_length_hint {
        -1 => None,
        value => Some(u64::try_from(value).map_err(|_| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "contentLengthHint must be -1 or non-negative",
            )
        })?),
    };

    Ok((concurrent, chunk, content_length_hint))
}

fn make_read_options<'a>(
    env: &mut Env<'a>,
    options: &JObject,
) -> Result<opendal::options::ReadOptions> {
    let offset = convert::read_int64_field(env, options, "offset")?;
    let length = convert::read_int64_field(env, options, "length")?;

    let concurrent = convert::read_int_field(env, options, "concurrent")?;
    let chunk = convert::read_int64_field(env, options, "chunk")?;
    let content_length_hint = convert::read_int64_field(env, options, "contentLengthHint")?;

    let (concurrent, chunk, content_length_hint) =
        validate_read_execution_options(concurrent, chunk, content_length_hint)?;

    Ok(opendal::options::ReadOptions {
        range: convert::offset_length_to_range(offset, length)?.into(),
        limit: None,
        version: convert::read_string_field(env, options, "version")?,
        if_match: convert::read_string_field(env, options, "ifMatch")?,
        if_none_match: convert::read_string_field(env, options, "ifNoneMatch")?,
        if_version_match: convert::read_string_field(env, options, "ifVersionMatch")?,
        if_version_not_match: convert::read_string_field(env, options, "ifVersionNotMatch")?,
        if_modified_since: convert::read_instant_field_to_timestamp(
            env,
            options,
            "ifModifiedSince",
        )?,
        if_unmodified_since: convert::read_instant_field_to_timestamp(
            env,
            options,
            "ifUnmodifiedSince",
        )?,
        concurrent,
        chunk,
        gap: None,
        content_length_hint,
        override_content_type: convert::read_string_field(env, options, "overrideContentType")?,
        override_cache_control: convert::read_string_field(env, options, "overrideCacheControl")?,
        override_content_disposition: convert::read_string_field(
            env,
            options,
            "overrideContentDisposition",
        )?,
    })
}

fn make_reader_options(
    env: &mut Env,
    options: &JObject,
) -> Result<opendal::options::ReaderOptions> {
    let concurrent = convert::read_int_field(env, options, "concurrent")?;
    let chunk = convert::read_int64_field(env, options, "chunk")?;
    let prefetch = convert::read_int_field(env, options, "prefetch")?;
    let content_length_hint = convert::read_int64_field(env, options, "contentLengthHint")?;

    let (concurrent, chunk, content_length_hint) =
        validate_read_execution_options(concurrent, chunk, content_length_hint)?;

    let prefetch = usize::try_from(prefetch)
        .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "prefetch must be non-negative"))?;

    Ok(opendal::options::ReaderOptions {
        version: convert::read_string_field(env, options, "version")?,
        if_match: convert::read_string_field(env, options, "ifMatch")?,
        if_none_match: convert::read_string_field(env, options, "ifNoneMatch")?,
        if_version_match: convert::read_string_field(env, options, "ifVersionMatch")?,
        if_version_not_match: convert::read_string_field(env, options, "ifVersionNotMatch")?,
        if_modified_since: convert::read_instant_field_to_timestamp(
            env,
            options,
            "ifModifiedSince",
        )?,
        if_unmodified_since: convert::read_instant_field_to_timestamp(
            env,
            options,
            "ifUnmodifiedSince",
        )?,
        concurrent,
        chunk,
        prefetch,
        content_length_hint,
        ..Default::default()
    })
}
