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
/// Capabilities reflects the native support for operations. It's possible
/// that some operations are not supported by current Operator, but still
/// can be used.
///
/// For examples, we will support `seek` and `next` for all readers
/// returned by services.
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
    /// If operator supports stat , it will be true.
    pub stat: bool,
    /// If operator supports stat with if match , it will be true.
    pub stat_with_if_match: bool,
    /// If operator supports stat with if none match , it will be true.
    pub stat_with_if_none_match: bool,

    /// If operator supports read , it will be true.
    pub read: bool,
    /// If operator supports seek on returning reader , it will
    /// be true.
    pub read_can_seek: bool,
    /// If operator supports next on returning reader , it will
    /// be true.
    pub read_can_next: bool,
    /// If operator supports read with range , it will be true.
    pub read_with_range: bool,
    /// If operator supports read with if match , it will be true.
    pub read_with_if_match: bool,
    /// If operator supports read with if none match , it will be true.
    pub read_with_if_none_match: bool,
    /// if operator supports read with override cache control , it will be true.
    pub read_with_override_cache_control: bool,
    /// if operator supports read with override content disposition , it will be true.
    pub read_with_override_content_disposition: bool,
    /// if operator supports read with override content type , it will be true.
    pub read_with_override_content_type: bool,

    /// If operator supports write , it will be true.
    pub write: bool,
    /// If operator supports write by append, it will be true.
    pub write_can_append: bool,
    /// If operator supports write with without content length, it will
    /// be true.
    ///
    /// This feature also be called as `Unsized` write or streaming write.
    pub write_without_content_length: bool,
    /// If operator supports write with content type , it will be true.
    pub write_with_content_type: bool,
    /// If operator supports write with content disposition , it will be true.
    pub write_with_content_disposition: bool,
    /// If operator supports write with cache control , it will be true.
    pub write_with_cache_control: bool,

    /// If operator supports create dir , it will be true.
    pub create_dir: bool,

    /// If operator supports delete , it will be true.
    pub delete: bool,

    /// If operator supports copy , it will be true.
    pub copy: bool,

    /// If operator supports rename , it will be true.
    pub rename: bool,

    /// If operator supports list , it will be true.
    pub list: bool,
    /// If backend supports list with limit, it will be true.
    pub list_with_limit: bool,
    /// If backend supports list with start after, it will be true.
    pub list_with_start_after: bool,
    /// If backend support list with using slash as delimiter.
    pub list_with_delimiter_slash: bool,
    /// If backend supports list without delimiter.
    pub list_without_delimiter: bool,

    /// If operator supports presign , it will be true.
    pub presign: bool,
    /// If operator supports presign read , it will be true.
    pub presign_read: bool,
    /// If operator supports presign stat , it will be true.
    pub presign_stat: bool,
    /// If operator supports presign write , it will be true.
    pub presign_write: bool,

    /// If operator supports batch , it will be true.
    pub batch: bool,
    /// If operator supports batch delete , it will be true.
    pub batch_delete: bool,
    /// The max operations that operator supports in batch.
    pub batch_max_operations: Option<usize>,

    /// If operator supports blocking , it will be true.
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
