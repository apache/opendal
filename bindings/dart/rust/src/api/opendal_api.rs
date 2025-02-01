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

use flutter_rust_bridge::frb;

use ::opendal as od;

use std::collections::HashMap;
use std::fmt::Display;
use std::io::Read;
use std::str::FromStr;
use std::time::Duration;

#[frb(opaque)]
pub struct Capability(opendal::Capability);

impl Capability {
    /// If operator supports stat.
    #[frb(sync, getter)]
    pub fn stat(&self) -> bool {
        self.0.stat
    }

    /// If operator supports stat with if matched.
    #[frb(sync, getter)]
    pub fn stat_with_if_match(&self) -> bool {
        self.0.stat_with_if_match
    }

    /// If operator supports stat with if not match.
    #[frb(sync, getter)]
    pub fn stat_with_if_none_match(&self) -> bool {
        self.0.stat_with_if_none_match
    }

    /// If operator supports read.
    #[frb(sync, getter)]
    pub fn read(&self) -> bool {
        self.0.read
    }

    /// If operator supports read with if matched.
    #[frb(sync, getter)]
    pub fn read_with_if_match(&self) -> bool {
        self.0.read_with_if_match
    }

    /// If operator supports read with if not match.
    #[frb(sync, getter)]
    pub fn read_with_if_none_match(&self) -> bool {
        self.0.read_with_if_none_match
    }

    /// if operator supports read with override cache control.
    #[frb(sync, getter)]
    pub fn read_with_override_cache_control(&self) -> bool {
        self.0.read_with_override_cache_control
    }

    /// if operator supports `read` with override content disposition.
    #[frb(sync, getter)]
    pub fn read_with_override_content_disposition(&self) -> bool {
        self.0.read_with_override_content_disposition
    }

    /// if operator supports read with override content type.
    #[frb(sync, getter)]
    pub fn read_with_override_content_type(&self) -> bool {
        self.0.read_with_override_content_type
    }

    /// If operator supports write.
    #[frb(sync, getter)]
    pub fn write(&self) -> bool {
        self.0.write
    }

    /// If operator supports write can be called in multi times.
    #[frb(sync, getter)]
    pub fn write_can_multi(&self) -> bool {
        self.0.write_can_multi
    }

    /// If operator supports write with empty content.
    #[frb(sync, getter)]
    pub fn write_can_empty(&self) -> bool {
        self.0.write_can_empty
    }

    /// If operator supports write by append.
    #[frb(sync, getter)]
    pub fn write_can_append(&self) -> bool {
        self.0.write_can_append
    }

    /// If operator supports write with content type.
    #[frb(sync, getter)]
    pub fn write_with_content_type(&self) -> bool {
        self.0.write_with_content_type
    }

    /// If operator supports write with content disposition.
    #[frb(sync, getter)]
    pub fn write_with_content_disposition(&self) -> bool {
        self.0.write_with_content_disposition
    }

    /// If operator supports write with cache control.
    #[frb(sync, getter)]
    pub fn write_with_cache_control(&self) -> bool {
        self.0.write_with_cache_control
    }

    /// write_multi_max_size is the max size that services support in write_multi.
    ///
    /// For example, AWS S3 supports 5GiB as max in write_multi.
    #[frb(sync, getter)]
    pub fn write_multi_max_size(&self) -> Option<usize> {
        self.0.write_multi_max_size
    }

    /// write_multi_min_size is the min size that services support in write_multi.
    ///
    /// For example, AWS S3 requires at least 5MiB in write_multi expect the last one.
    #[frb(sync, getter)]
    pub fn write_multi_min_size(&self) -> Option<usize> {
        self.0.write_multi_min_size
    }
    /// write_total_max_size is the max size that services support in write_total.
    ///
    /// For example, Cloudflare D1 supports 1MB as max in write_total.
    #[frb(sync, getter)]
    pub fn write_total_max_size(&self) -> Option<usize> {
        self.0.write_total_max_size
    }

    /// If operator supports create dir.
    #[frb(sync, getter)]
    pub fn create_dir(&self) -> bool {
        self.0.create_dir
    }

    /// If operator supports delete.
    #[frb(sync, getter)]
    pub fn delete(&self) -> bool {
        self.0.delete
    }

    /// If operator supports copy.
    #[frb(sync, getter)]
    pub fn copy(&self) -> bool {
        self.0.copy
    }

    /// If operator supports rename.
    #[frb(sync, getter)]
    pub fn rename(&self) -> bool {
        self.0.rename
    }

    /// If operator supports list.
    #[frb(sync, getter)]
    pub fn list(&self) -> bool {
        self.0.list
    }

    /// If backend supports list with limit.
    #[frb(sync, getter)]
    pub fn list_with_limit(&self) -> bool {
        self.0.list_with_limit
    }

    /// If backend supports list with start after.
    #[frb(sync, getter)]
    pub fn list_with_start_after(&self) -> bool {
        self.0.list_with_start_after
    }

    /// If backend supports list with recursive.
    #[frb(sync, getter)]
    pub fn list_with_recursive(&self) -> bool {
        self.0.list_with_recursive
    }

    /// If operator supports presign.
    #[frb(sync, getter)]
    pub fn presign(&self) -> bool {
        self.0.presign
    }

    /// If operator supports presign read.
    #[frb(sync, getter)]
    pub fn presign_read(&self) -> bool {
        self.0.presign_read
    }

    /// If operator supports presign stat.
    #[frb(sync, getter)]
    pub fn presign_stat(&self) -> bool {
        self.0.presign_stat
    }

    /// If operator supports presign write.
    #[frb(sync, getter)]
    pub fn presign_write(&self) -> bool {
        self.0.presign_write
    }

    /// If operator supports shared.
    #[frb(sync, getter)]
    pub fn shared(&self) -> bool {
        self.0.shared
    }

    /// If operator supports blocking.
    #[frb(sync, getter)]
    pub fn blocking(&self) -> bool {
        self.0.blocking
    }
}
