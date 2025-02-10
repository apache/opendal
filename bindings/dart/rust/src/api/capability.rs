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

struct Capability(opendal::Capability);

impl Capability {
    /// If operator supports stat.
    pub fn stat(&self) -> bool {
        self.0.stat
    }

    /// If operator supports stat with if matched.
    pub fn stat_with_if_match(&self) -> bool {
        self.0.stat_with_if_match
    }

    /// If operator supports stat with if not match.
    pub fn stat_with_if_none_match(&self) -> bool {
        self.0.stat_with_if_none_match
    }

    /// If operator supports read.
    pub fn read(&self) -> bool {
        self.0.read
    }

    /// If operator supports read with if matched.
    pub fn read_with_if_match(&self) -> bool {
        self.0.read_with_if_match
    }

    /// If operator supports read with if not match.
    pub fn read_with_if_none_match(&self) -> bool {
        self.0.read_with_if_none_match
    }

    /// if operator supports read with override cache control.
    pub fn read_with_override_cache_control(&self) -> bool {
        self.0.read_with_override_cache_control
    }

    /// if operator supports `read` with override content disposition.
    pub fn read_with_override_content_disposition(&self) -> bool {
        self.0.read_with_override_content_disposition
    }

    /// if operator supports read with override content type.
    pub fn read_with_override_content_type(&self) -> bool {
        self.0.read_with_override_content_type
    }

    /// If operator supports write.
    pub fn write(&self) -> bool {
        self.0.write
    }

    /// If operator supports write can be called in multi times.
    pub fn write_can_multi(&self) -> bool {
        self.0.write_can_multi
    }

    /// If operator supports write with empty content.
    pub fn write_can_empty(&self) -> bool {
        self.0.write_can_empty
    }

    /// If operator supports write by append.
    pub fn write_can_append(&self) -> bool {
        self.0.write_can_append
    }

    /// If operator supports write with content type.
    pub fn write_with_content_type(&self) -> bool {
        self.0.write_with_content_type
    }

    /// If operator supports write with content disposition.
    pub fn write_with_content_disposition(&self) -> bool {
        self.0.write_with_content_disposition
    }

    /// If operator supports write with cache control.
    pub fn write_with_cache_control(&self) -> bool {
        self.0.write_with_cache_control
    }

    /// write_multi_max_size is the max size that services support in write_multi.
    ///
    /// For example, AWS S3 supports 5GiB as max in write_multi.
    pub fn write_multi_max_size(&self) -> Option<usize> {
        self.0.write_multi_max_size
    }

    /// write_multi_min_size is the min size that services support in write_multi.
    ///
    /// For example, AWS S3 requires at least 5MiB in write_multi expect the last one.
    pub fn write_multi_min_size(&self) -> Option<usize> {
        self.0.write_multi_min_size
    }
    /// write_total_max_size is the max size that services support in write_total.
    ///
    /// For example, Cloudflare D1 supports 1MB as max in write_total.
    pub fn write_total_max_size(&self) -> Option<usize> {
        self.0.write_total_max_size
    }

    /// If operator supports create dir.
    pub fn create_dir(&self) -> bool {
        self.0.create_dir
    }

    /// If operator supports delete.
    pub fn delete(&self) -> bool {
        self.0.delete
    }

    /// If operator supports copy.
    pub fn copy(&self) -> bool {
        self.0.copy
    }

    /// If operator supports rename.
    pub fn rename(&self) -> bool {
        self.0.rename
    }

    /// If operator supports list.
    pub fn list(&self) -> bool {
        self.0.list
    }

    /// If backend supports list with limit.
    pub fn list_with_limit(&self) -> bool {
        self.0.list_with_limit
    }

    /// If backend supports list with start after.
    pub fn list_with_start_after(&self) -> bool {
        self.0.list_with_start_after
    }

    /// If backend supports list with recursive.
    pub fn list_with_recursive(&self) -> bool {
        self.0.list_with_recursive
    }

    /// If operator supports presign.
    pub fn presign(&self) -> bool {
        self.0.presign
    }

    /// If operator supports presign read.
    pub fn presign_read(&self) -> bool {
        self.0.presign_read
    }

    /// If operator supports presign stat.
    pub fn presign_stat(&self) -> bool {
        self.0.presign_stat
    }

    /// If operator supports presign write.
    pub fn presign_write(&self) -> bool {
        self.0.presign_write
    }

    /// If operator supports shared.
    pub fn shared(&self) -> bool {
        self.0.shared
    }

    /// If operator supports blocking.
    pub fn blocking(&self) -> bool {
        self.0.blocking
    }
}
