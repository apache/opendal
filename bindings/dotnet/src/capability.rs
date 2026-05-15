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

#[repr(C)]
#[derive(Default, Clone, Copy)]
/// FFI-safe mirror of `opendal::Capability` used by the .NET binding.
///
/// This struct intentionally mirrors fields from OpenDAL core capability,
/// but uses a stable C-compatible layout for cross-language interop.
///
/// We keep this dedicated mirror because Rust internal type layout is not an
/// FFI contract and must not be marshaled directly across the ABI boundary.
pub struct Capability {
    pub stat: bool,
    pub stat_with_if_match: bool,
    pub stat_with_if_none_match: bool,
    pub stat_with_if_modified_since: bool,
    pub stat_with_if_unmodified_since: bool,
    pub stat_with_override_cache_control: bool,
    pub stat_with_override_content_disposition: bool,
    pub stat_with_override_content_type: bool,
    pub stat_with_version: bool,

    pub read: bool,
    pub read_with_if_match: bool,
    pub read_with_if_none_match: bool,
    pub read_with_if_modified_since: bool,
    pub read_with_if_unmodified_since: bool,
    pub read_with_override_cache_control: bool,
    pub read_with_override_content_disposition: bool,
    pub read_with_override_content_type: bool,
    pub read_with_version: bool,

    pub write: bool,
    pub write_can_multi: bool,
    pub write_can_empty: bool,
    pub write_can_append: bool,
    pub write_with_content_type: bool,
    pub write_with_content_disposition: bool,
    pub write_with_content_encoding: bool,
    pub write_with_cache_control: bool,
    pub write_with_if_match: bool,
    pub write_with_if_none_match: bool,
    pub write_with_if_not_exists: bool,
    pub write_with_user_metadata: bool,

    pub write_multi_max_size: usize,
    pub write_multi_min_size: usize,
    pub write_total_max_size: usize,

    pub create_dir: bool,
    pub delete: bool,
    pub delete_with_version: bool,
    pub delete_with_recursive: bool,
    pub delete_max_size: usize,

    pub copy: bool,
    pub copy_with_if_not_exists: bool,
    pub rename: bool,

    pub list: bool,
    pub list_with_limit: bool,
    pub list_with_start_after: bool,
    pub list_with_recursive: bool,
    pub list_with_versions: bool,
    pub list_with_deleted: bool,

    pub presign: bool,
    pub presign_read: bool,
    pub presign_stat: bool,
    pub presign_write: bool,
    pub presign_delete: bool,

    pub shared: bool,
}

impl Capability {
    /// Convert OpenDAL core capability into the FFI mirror payload.
    pub fn new(cap: opendal::Capability) -> Self {
        Self {
            stat: cap.stat,
            stat_with_if_match: cap.stat_with_if_match,
            stat_with_if_none_match: cap.stat_with_if_none_match,
            stat_with_if_modified_since: cap.stat_with_if_modified_since,
            stat_with_if_unmodified_since: cap.stat_with_if_unmodified_since,
            stat_with_override_cache_control: cap.stat_with_override_cache_control,
            stat_with_override_content_disposition: cap.stat_with_override_content_disposition,
            stat_with_override_content_type: cap.stat_with_override_content_type,
            stat_with_version: cap.stat_with_version,
            read: cap.read,
            read_with_if_match: cap.read_with_if_match,
            read_with_if_none_match: cap.read_with_if_none_match,
            read_with_if_modified_since: cap.read_with_if_modified_since,
            read_with_if_unmodified_since: cap.read_with_if_unmodified_since,
            read_with_override_cache_control: cap.read_with_override_cache_control,
            read_with_override_content_disposition: cap.read_with_override_content_disposition,
            read_with_override_content_type: cap.read_with_override_content_type,
            read_with_version: cap.read_with_version,
            write: cap.write,
            write_can_multi: cap.write_can_multi,
            write_can_empty: cap.write_can_empty,
            write_can_append: cap.write_can_append,
            write_with_content_type: cap.write_with_content_type,
            write_with_content_disposition: cap.write_with_content_disposition,
            write_with_content_encoding: cap.write_with_content_encoding,
            write_with_cache_control: cap.write_with_cache_control,
            write_with_if_match: cap.write_with_if_match,
            write_with_if_none_match: cap.write_with_if_none_match,
            write_with_if_not_exists: cap.write_with_if_not_exists,
            write_with_user_metadata: cap.write_with_user_metadata,
            write_multi_max_size: cap.write_multi_max_size.unwrap_or(usize::MIN),
            write_multi_min_size: cap.write_multi_min_size.unwrap_or(usize::MIN),
            write_total_max_size: cap.write_total_max_size.unwrap_or(usize::MIN),
            create_dir: cap.create_dir,
            delete: cap.delete,
            delete_with_version: cap.delete_with_version,
            delete_with_recursive: cap.delete_with_recursive,
            delete_max_size: cap.delete_max_size.unwrap_or(usize::MIN),
            copy: cap.copy,
            copy_with_if_not_exists: cap.copy_with_if_not_exists,
            rename: cap.rename,
            list: cap.list,
            list_with_limit: cap.list_with_limit,
            list_with_start_after: cap.list_with_start_after,
            list_with_recursive: cap.list_with_recursive,
            list_with_versions: cap.list_with_versions,
            list_with_deleted: cap.list_with_deleted,
            presign: cap.presign,
            presign_read: cap.presign_read,
            presign_stat: cap.presign_stat,
            presign_write: cap.presign_write,
            presign_delete: cap.presign_delete,
            shared: cap.shared,
        }
    }
}
