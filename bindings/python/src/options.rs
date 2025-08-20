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

use dict_derive::FromPyObject;
use opendal::{self as ocore, raw::BytesRange};
use pyo3::pyclass;
use std::collections::HashMap;

use chrono::{DateTime, Utc};

#[pyclass(module = "opendal")]
#[derive(FromPyObject, Default)]
pub struct ReadOptions {
    pub version: Option<String>,
    pub concurrent: Option<usize>,
    pub chunk: Option<usize>,
    pub gap: Option<usize>,
    pub offset: Option<usize>,
    pub prefetch: Option<usize>,
    pub size: Option<usize>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<DateTime<Utc>>,
    pub if_unmodified_since: Option<DateTime<Utc>>,
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
}

impl ReadOptions {
    pub fn make_range(&self) -> BytesRange {
        let offset = self.offset.unwrap_or_default() as u64;
        let size = self.size.map(|v| v as u64);

        BytesRange::new(offset, size)
    }
}

#[pyclass(module = "opendal")]
#[derive(FromPyObject, Default)]
pub struct WriteOptions {
    pub append: Option<bool>,
    pub chunk: Option<usize>,
    pub concurrent: Option<usize>,
    pub cache_control: Option<String>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_not_exists: Option<bool>,
    pub user_metadata: Option<HashMap<String, String>>,
}

impl From<ReadOptions> for ocore::options::ReadOptions {
    fn from(opts: ReadOptions) -> Self {
        Self {
            range: opts.make_range(),
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since,
            if_unmodified_since: opts.if_unmodified_since,
            concurrent: opts.concurrent.unwrap_or_default(),
            chunk: opts.chunk,
            gap: opts.gap,
            override_content_type: opts.content_type,
            override_cache_control: opts.cache_control,
            override_content_disposition: opts.content_disposition,
        }
    }
}

impl From<ReadOptions> for ocore::options::ReaderOptions {
    fn from(opts: ReadOptions) -> Self {
        Self {
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since,
            if_unmodified_since: opts.if_unmodified_since,
            concurrent: opts.concurrent.unwrap_or_default(),
            chunk: opts.chunk,
            gap: opts.gap,
            prefetch: opts.prefetch.unwrap_or_default(),
        }
    }
}

impl From<WriteOptions> for ocore::options::WriteOptions {
    fn from(opts: WriteOptions) -> Self {
        Self {
            append: opts.append.unwrap_or(false),
            concurrent: opts.concurrent.unwrap_or_default(),
            chunk: opts.chunk,
            content_type: opts.content_type,
            content_disposition: opts.content_disposition,
            cache_control: opts.cache_control,
            content_encoding: opts.content_encoding,
            user_metadata: opts.user_metadata,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_not_exists: opts.if_not_exists.unwrap_or(false),
        }
    }
}

#[pyclass(module = "opendal")]
#[derive(FromPyObject, Default, Debug)]
pub struct ListOptions {
    pub limit: Option<usize>,
    pub start_after: Option<String>,
    pub recursive: Option<bool>,
    pub versions: Option<bool>,
    pub deleted: Option<bool>,
}

impl From<ListOptions> for ocore::options::ListOptions {
    fn from(opts: ListOptions) -> Self {
        Self {
            limit: opts.limit,
            start_after: opts.start_after,
            recursive: opts.recursive.unwrap_or(false),
            versions: opts.versions.unwrap_or(false),
            deleted: opts.deleted.unwrap_or(false),
        }
    }
}

#[pyclass(module = "opendal")]
#[derive(FromPyObject, Default, Debug)]
pub struct StatOptions {
    pub version: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<DateTime<Utc>>,
    pub if_unmodified_since: Option<DateTime<Utc>>,
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
}

impl From<StatOptions> for ocore::options::StatOptions {
    fn from(opts: StatOptions) -> Self {
        Self {
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since,
            if_unmodified_since: opts.if_unmodified_since,
            override_content_type: opts.content_type,
            override_cache_control: opts.cache_control,
            override_content_disposition: opts.content_disposition,
        }
    }
}
