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

/// Capability is used to describe what operations are supported
/// by current Operator.
#[pyclass(get_all, module = "opendal")]
pub struct Capability {
    /// If operator supports stat.
    pub stat: bool,
    /// If operator supports stat with if match.
    pub stat_with_if_match: bool,
    /// If operator supports stat with if none match.
    pub stat_with_if_none_match: bool,

    /// If operator supports read.
    pub read: bool,
    /// If operator supports read with if match.
    pub read_with_if_match: bool,
    /// If operator supports read with if none match.
    pub read_with_if_none_match: bool,
    /// if operator supports read with override cache control.
    pub read_with_override_cache_control: bool,
    /// if operator supports read with override content disposition.
    pub read_with_override_content_disposition: bool,
    /// if operator supports read with override content type.
    pub read_with_override_content_type: bool,

    /// If operator supports write.
    pub write: bool,
    /// If operator supports write can be called in multi times.
    pub write_can_multi: bool,
    /// If operator supports write with empty content.
    pub write_can_empty: bool,
    /// If operator supports write by append.
    pub write_can_append: bool,
    /// If operator supports write with content type.
    pub write_with_content_type: bool,
    /// If operator supports write with content disposition.
    pub write_with_content_disposition: bool,
    /// If operator supports write with cache control.
    pub write_with_cache_control: bool,
    /// write_multi_max_size is the max size that services support in write_multi.
    ///
    /// For example, AWS S3 supports 5GiB as max in write_multi.
    pub write_multi_max_size: Option<usize>,
    /// write_multi_min_size is the min size that services support in write_multi.
    ///
    /// For example, AWS S3 requires at least 5MiB in write_multi expect the last one.
    pub write_multi_min_size: Option<usize>,
    /// write_total_max_size is the max size that services support in write_total.
    ///
    /// For example, Cloudflare D1 supports 1MB as max in write_total.
    pub write_total_max_size: Option<usize>,

    /// If operator supports create dir.
    pub create_dir: bool,

    /// If operator supports delete.
    pub delete: bool,

    /// If operator supports copy.
    pub copy: bool,

    /// If operator supports rename.
    pub rename: bool,

    /// If operator supports list.
    pub list: bool,
    /// If backend supports list with limit.
    pub list_with_limit: bool,
    /// If backend supports list with start after.
    pub list_with_start_after: bool,
    /// If backend supports list without delimiter.
    pub list_with_recursive: bool,

    /// If operator supports presign.
    pub presign: bool,
    /// If operator supports presign read.
    pub presign_read: bool,
    /// If operator supports presign stat.
    pub presign_stat: bool,
    /// If operator supports presign write.
    pub presign_write: bool,
    /// If operator supports presign delete.
    pub presign_delete: bool,

    /// If operator supports shared.
    pub shared: bool,

    /// If operator supports blocking.
    pub blocking: bool,
}

impl Capability {
    pub fn new(capability: opendal::Capability) -> Self {
        Self {
            stat: capability.stat,
            stat_with_if_match: capability.stat_with_if_match,
            stat_with_if_none_match: capability.stat_with_if_none_match,
            read: capability.read,
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
            write_total_max_size: capability.write_total_max_size,
            create_dir: capability.create_dir,
            delete: capability.delete,
            copy: capability.copy,
            rename: capability.rename,
            list: capability.list,
            list_with_limit: capability.list_with_limit,
            list_with_start_after: capability.list_with_start_after,
            list_with_recursive: capability.list_with_recursive,
            presign: capability.presign,
            presign_read: capability.presign_read,
            presign_stat: capability.presign_stat,
            presign_write: capability.presign_write,
            presign_delete: capability.presign_delete,
            shared: capability.shared,
            blocking: capability.blocking,
        }
    }
}
