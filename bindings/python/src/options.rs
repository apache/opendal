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

use crate::format_pyerr;
use crate::ocore::{Operator, Reader, Writer};
use dict_derive::FromPyObject;
use pyo3::prelude::PyResult;
use pyo3::pyclass;
use std::collections::HashMap;

use std::ops::Bound as RangeBound;

#[pyclass(module = "opendal")]
#[derive(FromPyObject, Default)]
pub struct ReaderOptions {
    pub version: Option<String>,
    pub concurrent: Option<usize>,
    pub chunk: Option<usize>,
    pub gap: Option<usize>,
    pub range_start: Option<usize>,
    pub range_end: Option<usize>,
}

impl ReaderOptions {
    pub fn make_range(&self) -> (RangeBound<u64>, RangeBound<u64>) {
        let start_bound = self
            .range_start
            .map_or(RangeBound::Unbounded, |s| RangeBound::Included(s as u64));
        let end_bound = self
            .range_end
            .map_or(RangeBound::Unbounded, |e| RangeBound::Excluded(e as u64));

        (start_bound, end_bound)
    }

    pub async fn create_reader(&self, op: &Operator, path: String) -> PyResult<Reader> {
        let mut fr = op.reader_with(&path);

        if let Some(version) = &self.version {
            fr = fr.version(version);
        };
        if let Some(concurrent) = self.concurrent {
            fr = fr.concurrent(concurrent);
        };
        if let Some(chunk) = self.chunk {
            fr = fr.chunk(chunk);
        };
        if let Some(gap) = self.gap {
            fr = fr.gap(gap);
        };

        let reader = fr.await.map_err(format_pyerr)?;
        Ok(reader)
    }
}

#[pyclass(module = "opendal")]
#[derive(FromPyObject, Default)]
pub struct WriterOptions {
    pub append: Option<bool>,
    pub chunk: Option<usize>,
    pub concurrent: Option<usize>,
    pub cache_control: Option<String>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub if_not_exists: Option<bool>,
    pub user_metadata: Option<HashMap<String, String>>,
}

impl WriterOptions {
    pub async fn create_writer(&self, op: &Operator, path: String) -> PyResult<Writer> {
        let mut fw = op.writer_with(&path);

        if let Some(append) = self.append {
            fw = fw.append(append);
        };
        if let Some(chunk) = self.chunk {
            fw = fw.chunk(chunk);
        };
        if let Some(concurrent) = self.concurrent {
            fw = fw.concurrent(concurrent);
        };
        if let Some(cache_control) = &self.cache_control {
            fw = fw.cache_control(cache_control);
        };
        if let Some(content_type) = &self.content_type {
            fw = fw.content_type(content_type);
        };
        if let Some(content_disposition) = &self.content_disposition {
            fw = fw.content_disposition(content_disposition);
        };
        if let Some(content_encoding) = &self.content_encoding {
            fw = fw.content_encoding(content_encoding);
        };
        if let Some(if_not_exists) = self.if_not_exists {
            fw = fw.if_not_exists(if_not_exists);
        };
        if let Some(user_metadata) = &self.user_metadata {
            fw = fw.user_metadata(user_metadata.clone());
        };

        let writer = fw.await.map_err(format_pyerr)?;
        Ok(writer)
    }
}
