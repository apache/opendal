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

use ::opendal as core;
use opendal::{
    options::ReadOptions,
    raw::{BytesRange, Timestamp},
    Error,
};
use std::{os::raw::c_char, str::FromStr};

/// \brief Options for read operations used by C side.
///
/// \note For detail description of each field, please refer to [`core::ReadOptions`]  
#[repr(C)]
#[derive(Clone, Copy)]
pub struct opendal_operator_options_read {
    /// Set `range` for this operation.
    pub range: *const u64,
    /// Set `version` for this operation.
    pub version: *const c_char,
    /// Set `if_match` for this operation.
    pub if_match: *const c_char,
    /// Set `if_none_match` for this operation.
    pub if_none_match: *const c_char,
    /// Set `if_modified_since` for this operation.
    ///
    /// \note The value should be in RFC 3339 format.
    pub if_modified_since: *const c_char,
    /// Set `if_unmodified_since` for this operation.
    ///
    /// \note The value should be in RFC 3339 format.
    pub if_unmodified_since: *const c_char,
    /// Set `concurrent` for the operation.
    ///
    /// \note for we do not provide default value in C, so it must be Option in C side.
    pub concurrent: *const usize,
    /// Set `chunk` for the operation.
    pub chunk: *const usize,
    /// Controls the optimization strategy for range reads in [`Reader::fetch`].
    pub gap: *const usize,
    /// Specify the content-type header that should be sent back by the operation.
    pub override_content_type: *const c_char,
    /// Specify the `cache-control` header that should be sent back by the operation.
    pub override_cache_control: *const c_char,
    /// Specify the `content-disposition` header that should be sent back by the operation.
    pub override_content_disposition: *const c_char,
}

impl opendal_operator_options_read {}

pub fn parse_read_options(
    options: *const opendal_operator_options_read,
) -> Result<core::options::ReadOptions, Error> {
    // if orginal opts is blank, we will use the default options
    let mut opts = ReadOptions::default();

    unsafe {
        let options = *options;
        if !options.range.is_null() {
            // TODO:
            // Do we need to make sure it has no more than 2 usize?
            let range = std::slice::from_raw_parts(options.range, 2);
            opts.range = BytesRange::new(range[0], Some(range[1]));
        }
        if !options.version.is_null() {
            opts.version = Some(
                std::ffi::CStr::from_ptr(options.version)
                    .to_str()
                    .expect("malformed version")
                    .to_string(),
            );
        }
        if !options.if_match.is_null() {
            opts.if_match = Some(
                std::ffi::CStr::from_ptr(options.if_match)
                    .to_str()
                    .expect("malformed if_match")
                    .to_string(),
            );
        }
        if !options.if_none_match.is_null() {
            opts.if_none_match = Some(
                std::ffi::CStr::from_ptr(options.if_none_match)
                    .to_str()
                    .expect("malformed if_none_match")
                    .to_string(),
            );
        }
        if !options.if_modified_since.is_null() {
            let ts_str = std::ffi::CStr::from_ptr(options.if_modified_since)
                .to_str()
                .expect("malformed if_modified_since")
                .to_string();

            let ts = match Timestamp::from_str(&ts_str) {
                Ok(ts) => ts,
                Err(e) => return Err(e),
            };
            opts.if_modified_since = Some(ts);
        }
        if !options.if_unmodified_since.is_null() {
            let ts_str = std::ffi::CStr::from_ptr(options.if_unmodified_since)
                .to_str()
                .expect("malformed if_unmodified_since")
                .to_string();

            let ts = match Timestamp::from_str(&ts_str) {
                Ok(ts) => ts,
                Err(e) => return Err(e),
            };
            opts.if_unmodified_since = Some(ts);
        }
        if !options.concurrent.is_null() {
            opts.concurrent = *options.concurrent;
        }
        if !options.chunk.is_null() {
            opts.chunk = Some(*options.chunk);
        }
        if !options.gap.is_null() {
            opts.gap = Some(*options.gap);
        }
        if !options.override_content_type.is_null() {
            opts.override_content_type = Some(
                std::ffi::CStr::from_ptr(options.override_content_type)
                    .to_str()
                    .expect("malformed override_content_type")
                    .to_string(),
            );
        }
        if !options.override_cache_control.is_null() {
            opts.override_cache_control = Some(
                std::ffi::CStr::from_ptr(options.override_cache_control)
                    .to_str()
                    .expect("malformed override_cache_control")
                    .to_string(),
            );
        }
        if !options.override_content_disposition.is_null() {
            opts.override_content_disposition = Some(
                std::ffi::CStr::from_ptr(options.override_content_disposition)
                    .to_str()
                    .expect("malformed override_content_disposition")
                    .to_string(),
            );
        }
    }

    Ok(opts)
}
