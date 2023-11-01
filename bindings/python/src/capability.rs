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

use pyo3::prelude::*;

#[pyclass(get_all, module = "opendal")]
pub struct Capability {
    pub stat: bool,
    pub stat_with_if_match: bool,
    pub stat_with_if_none_match: bool,
    pub read: bool,
    pub read_can_seek: bool,
    pub read_can_next: bool,
    pub read_with_range: bool,
    pub read_with_if_match: bool,
    pub read_with_if_none_match: bool,
    pub read_with_override_cache_control: bool,
    pub read_with_override_content_disposition: bool,
    pub read_with_override_content_type: bool,
    pub write: bool,
    pub write_can_multi: bool,
    pub write_can_empty: bool,
    pub write_can_append: bool,
    pub write_with_content_type: bool,
    pub write_with_content_disposition: bool,
    pub write_with_cache_control: bool,
    pub write_multi_max_size: Option<usize>,
    pub write_multi_min_size: Option<usize>,
    pub write_multi_align_size: Option<usize>,
    pub write_total_max_size: Option<usize>,
    pub create_dir: bool,
    pub delete: bool,
    pub copy: bool,
    pub rename: bool,
    pub list: bool,
    pub list_with_limit: bool,
    pub list_with_start_after: bool,
    pub list_with_delimiter_slash: bool,
    pub list_without_delimiter: bool,
    pub presign: bool,
    pub presign_read: bool,
    pub presign_stat: bool,
    pub presign_write: bool,
    pub batch: bool,
    pub batch_delete: bool,
    pub batch_max_operations: Option<usize>,
    pub blocking: bool,
}

impl Capability {
    pub fn new(capability: opendal::Capability) -> Self {
        Self {
            stat: capability.stat,
            stat_with_if_match: capability.stat_with_if_match,
            stat_with_if_none_match: capability.stat_with_if_none_match,
            read: capability.read,
            read_can_seek: capability.read_can_seek,
            read_can_next: capability.read_can_next,
            read_with_range: capability.read_with_range,
            read_with_if_match: capability.read_with_if_match,
            read_with_if_none_match: capability.read_with_if_none_match,
            read_with_override_cache_control: capability.read_with_override_cache_control,
            read_with_override_content_disposition: capability
                .read_with_override_content_disposition,
            read_with_override_content_type: capability.read_with_override_content_type,
            write: capability.write,
            write_can_multi: capability.write_can_multi,
            write_can_empty: capability.write_can_empty,
            write_can_append: capability.write_can_append,
            write_with_content_type: capability.write_with_content_type,
            write_with_content_disposition: capability.write_with_content_disposition,
            write_with_cache_control: capability.write_with_cache_control,
            write_multi_max_size: capability.write_multi_max_size,
            write_multi_min_size: capability.write_multi_min_size,
            write_multi_align_size: capability.write_multi_align_size,
            write_total_max_size: capability.write_total_max_size,
            create_dir: capability.create_dir,
            delete: capability.delete,
            copy: capability.copy,
            rename: capability.rename,
            list: capability.list,
            list_with_limit: capability.list_with_limit,
            list_with_start_after: capability.list_with_start_after,
            list_with_delimiter_slash: capability.list_with_delimiter_slash,
            list_without_delimiter: capability.list_without_delimiter,
            presign: capability.presign,
            presign_read: capability.presign_read,
            presign_stat: capability.presign_stat,
            presign_write: capability.presign_write,
            batch: capability.batch,
            batch_delete: capability.batch_delete,
            batch_max_operations: capability.batch_max_operations,
            blocking: capability.blocking,
        }
    }
}
