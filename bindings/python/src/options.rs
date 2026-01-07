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
use pyo3::prelude::*;
use pyo3_stub_gen::derive::*;
use std::collections::HashMap;

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
    pub if_modified_since: Option<jiff::Timestamp>,
    pub if_unmodified_since: Option<jiff::Timestamp>,
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
#[derive(Clone, Default)]
#[gen_stub_pyclass]
pub struct WriteOptions {
    #[pyo3(get, set)]
    pub append: Option<bool>,
    #[pyo3(get, set)]
    pub chunk: Option<usize>,
    #[pyo3(get, set)]
    pub concurrent: Option<usize>,
    #[pyo3(get, set)]
    pub cache_control: Option<String>,
    #[pyo3(get, set)]
    pub content_type: Option<String>,
    #[pyo3(get, set)]
    pub content_disposition: Option<String>,
    #[pyo3(get, set)]
    pub content_encoding: Option<String>,
    #[pyo3(get, set)]
    pub if_match: Option<String>,
    #[pyo3(get, set)]
    pub if_none_match: Option<String>,
    #[pyo3(get, set)]
    pub if_not_exists: Option<bool>,
    #[pyo3(get, set)]
    pub user_metadata: Option<HashMap<String, String>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl WriteOptions {
    #[new]
    #[pyo3(signature = (*, append=None, chunk=None, concurrent=None, cache_control=None, content_type=None, content_disposition=None, content_encoding=None, if_match=None, if_none_match=None, if_not_exists=None, user_metadata=None))]
    fn new(
        append: Option<bool>,
        chunk: Option<usize>,
        concurrent: Option<usize>,
        cache_control: Option<String>,
        content_type: Option<String>,
        content_disposition: Option<String>,
        content_encoding: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
        if_not_exists: Option<bool>,
        user_metadata: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            append,
            chunk,
            concurrent,
            cache_control,
            content_type,
            content_disposition,
            content_encoding,
            if_match,
            if_none_match,
            if_not_exists,
            user_metadata,
        }
    }
}

impl From<ReadOptions> for ocore::options::ReadOptions {
    fn from(opts: ReadOptions) -> Self {
        Self {
            range: opts.make_range(),
            version: opts.version,
            if_match: opts.if_match,
            if_none_match: opts.if_none_match,
            if_modified_since: opts.if_modified_since.map(Into::into),
            if_unmodified_since: opts.if_unmodified_since.map(Into::into),
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
            if_modified_since: opts.if_modified_since.map(Into::into),
            if_unmodified_since: opts.if_unmodified_since.map(Into::into),
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
    pub if_modified_since: Option<jiff::Timestamp>,
    pub if_unmodified_since: Option<jiff::Timestamp>,
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
            if_modified_since: opts.if_modified_since.map(Into::into),
            if_unmodified_since: opts.if_unmodified_since.map(Into::into),
            override_content_type: opts.content_type,
            override_cache_control: opts.cache_control,
            override_content_disposition: opts.content_disposition,
        }
    }
}
