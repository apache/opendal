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

use std::fmt::Debug;

/// Capability is used to describe what operations are supported
/// by current Operator.
///
/// Via capability, we can know:
///
/// - Whether current Operator supports read or not.
/// - Whether current Operator supports read with if match or not.
/// - What's current Operator max supports batch operations count.
///
/// Add fields of Capabilities with be public and can be accessed directly.
///
/// # Notes
///
/// Every operator has two kinds of capabilities:
///
/// - [`OperatorInfo::native_capability`][crate::OperatorInfo::native_capability] reflects the native
///   support for operations.
/// - [`OperatorInfo::full_capability`][crate::OperatorInfo::full_capability] reflects the full support
///   for operations.
///
/// It's possible that some operations are not supported by current Operator, but still
/// can be used. For examples:
///
/// - S3 doesn't support `seek` natively, but we implement it via `range` header.
/// - S3 doesn't support blocking API, but `BlockingLayer` makes it possible.
///
/// Users can use full_capability to decide what operations can be used and use native_capability to
/// decide if this operation optimized or not.
///
/// # Naming Style
///
/// - Operation itself should be in lower case, like `read`, `write`.
/// - Operation with sub operations should be named like `presign_read`.
/// - Operation with variants should be named like `read_can_seek`.
/// - Operation with arguments should be named like `read_with_range`.
/// - Operation with limitations should be named like `batch_max_operations`.
#[derive(Copy, Clone, Default)]
pub struct Capability {
    /// If operator supports stat.
    pub stat: bool,
    /// If operator supports stat with if match.
    pub stat_with_if_match: bool,
    /// If operator supports stat with if none match.
    pub stat_with_if_none_match: bool,
    /// if operator supports stat with override cache control.
    pub stat_with_override_cache_control: bool,
    /// if operator supports stat with override content disposition.
    pub stat_with_override_content_disposition: bool,
    /// if operator supports stat with override content type.
    pub stat_with_override_content_type: bool,
    /// if operator supports stat with versioning.
    pub stat_with_version: bool,

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
    /// if operator supports read with versioning.
    pub read_with_version: bool,

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
    /// If operator supports write with user defined metadata
    pub write_with_user_metadata: bool,
    /// write_multi_max_size is the max size that services support in write_multi.
    ///
    /// For example, AWS S3 supports 5GiB as max in write_multi.
    pub write_multi_max_size: Option<usize>,
    /// write_multi_min_size is the min size that services support in write_multi.
    ///
    /// For example, AWS S3 requires at least 5MiB in write_multi expect the last one.
    pub write_multi_min_size: Option<usize>,
    /// write_multi_align_size is the align size that services required in write_multi.
    ///
    /// For example, Google GCS requires align size to 256KiB in write_multi.
    pub write_multi_align_size: Option<usize>,
    /// write_total_max_size is the max size that services support in write_total.
    ///
    /// For example, Cloudflare D1 supports 1MB as max in write_total.
    pub write_total_max_size: Option<usize>,

    /// If operator supports create dir.
    pub create_dir: bool,

    /// If operator supports delete.
    pub delete: bool,
    /// if operator supports delete with versioning.
    pub delete_with_version: bool,

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
    /// If backend supports list with recursive.
    pub list_with_recursive: bool,
    /// if operator supports list with versioning.
    pub list_with_version: bool,

    /// If operator supports presign.
    pub presign: bool,
    /// If operator supports presign read.
    pub presign_read: bool,
    /// If operator supports presign stat.
    pub presign_stat: bool,
    /// If operator supports presign write.
    pub presign_write: bool,

    /// If operator supports batch.
    pub batch: bool,
    /// If operator supports batch delete.
    pub batch_delete: bool,
    /// The max operations that operator supports in batch.
    pub batch_max_operations: Option<usize>,

    /// If operator supports blocking.
    pub blocking: bool,
}

impl Debug for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = vec![];

        if self.stat {
            s.push("Stat");
        }
        if self.read {
            s.push("Read");
        }
        if self.write {
            s.push("Write");
        }
        if self.create_dir {
            s.push("CreateDir");
        }
        if self.delete {
            s.push("Delete");
        }
        if self.copy {
            s.push("Copy");
        }
        if self.rename {
            s.push("Rename");
        }
        if self.list {
            s.push("List");
        }
        if self.presign {
            s.push("Presign");
        }
        if self.batch {
            s.push("Batch");
        }
        if self.blocking {
            s.push("Blocking");
        }

        write!(f, "{{ {} }}", s.join(" | "))
    }
}
