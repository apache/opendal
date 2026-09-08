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
use opendal::OperatorInfo;
use opendal::raw::PresignedRequest;
use opendal::{Capability, Error, ErrorKind};

mod async_operator;
mod convert;
mod error;
mod executor;
mod layer;
mod operator;
mod operator_input_stream;
mod operator_output_stream;
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
    let capability_obj = make_capability(env, info.capability())?;

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

fn make_capability<'a>(env: &mut Env<'a>, cap: Capability) -> Result<JObject<'a>> {
    let capability = env.new_object(
        jni_str!("org/apache/opendal/Capability"),
        jni_sig!("(ZZZZZZZZZZZZZZZZZZZZZZJJZZZZZZZZZZZZZZZ)V"),
        &[
            JValue::Bool(cap.stat),
            JValue::Bool(cap.stat_with_if_match),
            JValue::Bool(cap.stat_with_if_none_match),
            JValue::Bool(cap.stat_with_if_modified_since),
            JValue::Bool(cap.stat_with_if_unmodified_since),
            JValue::Bool(cap.stat_with_version),
            JValue::Bool(cap.read),
            JValue::Bool(cap.read_with_if_match),
            JValue::Bool(cap.read_with_if_none_match),
            JValue::Bool(cap.read_with_override_cache_control),
            JValue::Bool(cap.read_with_override_content_disposition),
            JValue::Bool(cap.read_with_override_content_type),
            JValue::Bool(cap.write),
            JValue::Bool(cap.write_can_multi),
            JValue::Bool(cap.write_can_append),
            JValue::Bool(cap.write_with_content_type),
            JValue::Bool(cap.write_with_content_disposition),
            JValue::Bool(cap.write_with_cache_control),
            JValue::Bool(cap.write_with_if_match),
            JValue::Bool(cap.write_with_if_none_match),
            JValue::Bool(cap.write_with_if_not_exists),
            JValue::Bool(cap.write_with_user_metadata),
            JValue::Long(convert::usize_to_jlong(cap.write_multi_max_size)),
            JValue::Long(convert::usize_to_jlong(cap.write_multi_min_size)),
            JValue::Bool(cap.create_dir),
            JValue::Bool(cap.delete),
            JValue::Bool(cap.copy),
            JValue::Bool(cap.rename),
            JValue::Bool(cap.list),
            JValue::Bool(cap.list_with_limit),
            JValue::Bool(cap.list_with_start_after),
            JValue::Bool(cap.list_with_recursive),
            JValue::Bool(cap.list_with_versions),
            JValue::Bool(cap.list_with_deleted),
            JValue::Bool(cap.presign),
            JValue::Bool(cap.presign_read),
            JValue::Bool(cap.presign_stat),
            JValue::Bool(cap.presign_write),
            JValue::Bool(cap.shared),
        ],
    )?;
    Ok(capability)
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

    let result = env
        .new_object(
            jni_str!("org/apache/opendal/Metadata"),
            jni_sig!(
                "(IJLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/time/Instant;Ljava/lang/String;)V"
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
    })
}

fn make_stat_options(env: &mut Env, options: &JObject) -> Result<opendal::options::StatOptions> {
    Ok(opendal::options::StatOptions {
        if_match: convert::read_string_field(env, options, "ifMatch")?,
        if_none_match: convert::read_string_field(env, options, "ifNoneMatch")?,
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
        ..Default::default()
    })
}

fn make_read_options<'a>(
    env: &mut Env<'a>,
    options: &JObject,
) -> Result<opendal::options::ReadOptions> {
    let offset = convert::read_int64_field(env, options, "offset")?;
    let length = convert::read_int64_field(env, options, "length")?;

    Ok(opendal::options::ReadOptions {
        range: convert::offset_length_to_range(offset, length)?.into(),
        ..Default::default()
    })
}

fn make_reader_options(
    env: &mut Env,
    options: &JObject,
) -> Result<opendal::options::ReaderOptions> {
    Ok(build_reader_options(
        convert::read_int_field(env, options, "concurrent")?,
        convert::read_int64_field(env, options, "chunk")?,
        convert::read_int_field(env, options, "prefetch")?,
        convert::read_int64_field(env, options, "contentLengthHint")?,
    )?)
}

fn build_reader_options(
    concurrent: i32,
    chunk: i64,
    prefetch: i32,
    content_length_hint: i64,
) -> opendal::Result<opendal::options::ReaderOptions> {
    use opendal::{Error, ErrorKind};

    if concurrent <= 0 {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            "concurrent must be positive",
        ));
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
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "chunk must be -1 or positive",
            ));
        }
    };
    let prefetch = usize::try_from(prefetch)
        .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "prefetch must be non-negative"))?;
    let content_length_hint = match content_length_hint {
        -1 => None,
        value => Some(u64::try_from(value).map_err(|_| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "contentLengthHint must be -1 or non-negative",
            )
        })?),
    };

    Ok(opendal::options::ReaderOptions {
        concurrent,
        chunk,
        prefetch,
        content_length_hint,
        ..Default::default()
    })
}

#[cfg(test)]
mod reader_options_tests {
    use super::build_reader_options;
    use opendal::ErrorKind;

    #[test]
    fn default_reader_options() {
        let options = build_reader_options(1, -1, 0, -1).unwrap();
        assert_eq!(options.concurrent, 1);
        assert_eq!(options.chunk, None);
        assert_eq!(options.prefetch, 0);
        assert_eq!(options.content_length_hint, None);
    }

    #[test]
    fn tuned_reader_options() {
        let options = build_reader_options(4, 8 * 1024 * 1024, 2, 128 * 1024 * 1024).unwrap();
        assert_eq!(options.concurrent, 4);
        assert_eq!(options.chunk, Some(8 * 1024 * 1024));
        assert_eq!(options.prefetch, 2);
        assert_eq!(options.content_length_hint, Some(128 * 1024 * 1024));
    }

    #[test]
    fn empty_content_length_hint() {
        let options = build_reader_options(1, 1, 0, 0).unwrap();
        assert_eq!(options.content_length_hint, Some(0));
    }

    #[test]
    fn invalid_reader_options() {
        for (concurrent, chunk, prefetch, hint, field) in [
            (0, -1, 0, -1, "concurrent"),
            (-1, -1, 0, -1, "concurrent"),
            (1, 0, 0, -1, "chunk"),
            (1, -2, 0, -1, "chunk"),
            (1, -1, -1, -1, "prefetch"),
            (1, -1, 0, -2, "contentLengthHint"),
        ] {
            let err = build_reader_options(concurrent, chunk, prefetch, hint).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
            assert!(err.message().contains(field));
        }
    }

    #[test]
    fn chunk_conversion_respects_native_width() {
        let options = build_reader_options(1, i64::MAX, 0, -1);
        if usize::BITS < 64 {
            assert_eq!(options.unwrap_err().kind(), ErrorKind::ConfigInvalid);
        } else {
            assert_eq!(options.unwrap().chunk, usize::try_from(i64::MAX).ok());
        }
    }
}
