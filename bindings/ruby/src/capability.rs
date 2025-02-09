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

use magnus::class;
use magnus::method;
use magnus::prelude::*;
use magnus::Error;
use magnus::RModule;

use crate::*;

// This name follows `attr_accessor` in Ruby
macro_rules! define_accessors {
    ($struct:ty, { $( $field:ident : $type:ty ),+ $(,)? }) => {
        impl $struct {
            $(
                pub fn $field(&self) -> $type {
                    self.0.$field
                }
            )+
        }
    };
}
macro_rules! bind_methods_to_ruby {
    ($ruby_class:ident, { $( $field:ident ),+ $(,)? }) => {
        $(
            $ruby_class.define_method(stringify!($field), method!(Capability::$field, 0))?;
        )+
    };
}

/// Capability describes OpenDAL supported operations by current Operator.
#[magnus::wrap(class = "OpenDAL::Capability", free_immediately, size)]
pub struct Capability(ocore::Capability);

impl Capability {
    pub fn new(capability: ocore::Capability) -> Self {
        Self(capability)
    }
}

define_accessors!(Capability, {
    stat: bool,
    stat_with_if_match: bool,
    stat_with_if_none_match: bool,
    stat_with_if_modified_since: bool,
    stat_with_if_unmodified_since: bool,
    stat_with_override_cache_control: bool,
    stat_with_override_content_disposition: bool,
    stat_with_override_content_type: bool,
    stat_with_version: bool,
    stat_has_cache_control: bool,
    stat_has_content_disposition: bool,
    stat_has_content_length: bool,
    stat_has_content_md5: bool,
    stat_has_content_range: bool,
    stat_has_content_type: bool,
    stat_has_content_encoding: bool,
    stat_has_etag: bool,
    stat_has_last_modified: bool,
    stat_has_version: bool,
    stat_has_user_metadata: bool,
    read: bool,
    read_with_if_match: bool,
    read_with_if_none_match: bool,
    read_with_if_modified_since: bool,
    read_with_if_unmodified_since: bool,
    read_with_override_cache_control: bool,
    read_with_override_content_disposition: bool,
    read_with_override_content_type: bool,
    read_with_version: bool,
    write: bool,
    write_can_multi: bool,
    write_can_empty: bool,
    write_can_append: bool,
    write_with_content_type: bool,
    write_with_content_disposition: bool,
    write_with_content_encoding: bool,
    write_with_cache_control: bool,
    write_with_if_match: bool,
    write_with_if_none_match: bool,
    write_with_if_not_exists: bool,
    write_with_user_metadata: bool,
    write_multi_max_size: Option<usize>,
    write_multi_min_size: Option<usize>,
    write_total_max_size: Option<usize>,
    create_dir: bool,
    delete: bool,
    delete_with_version: bool,
    delete_max_size: Option<usize>,
    copy: bool,
    rename: bool,
    list: bool,
    list_with_limit: bool,
    list_with_start_after: bool,
    list_with_recursive: bool,
    list_with_versions: bool,
    list_with_deleted: bool,
    list_has_cache_control: bool,
    list_has_content_disposition: bool,
    list_has_content_length: bool,
    list_has_content_md5: bool,
    list_has_content_range: bool,
    list_has_content_type: bool,
    list_has_etag: bool,
    list_has_last_modified: bool,
    list_has_version: bool,
    list_has_user_metadata: bool,
    presign: bool,
    presign_read: bool,
    presign_stat: bool,
    presign_write: bool,
    shared: bool,
    blocking: bool,
});

// includes class into the Ruby module
pub fn include(gem_module: &RModule) -> Result<(), Error> {
    let class = gem_module.define_class("Capability", class::object())?;
    bind_methods_to_ruby!(class, {
        stat,
        stat_with_if_match,
        stat_with_if_none_match,
        stat_with_if_modified_since,
        stat_with_if_unmodified_since,
        stat_with_override_cache_control,
        stat_with_override_content_disposition,
        stat_with_override_content_type,
        stat_with_version,
        stat_has_cache_control,
        stat_has_content_disposition,
        stat_has_content_length,
        stat_has_content_md5,
        stat_has_content_range,
        stat_has_content_type,
        stat_has_content_encoding,
        stat_has_etag,
        stat_has_last_modified,
        stat_has_version,
        stat_has_user_metadata,
        read,
        read_with_if_match,
        read_with_if_none_match,
        read_with_if_modified_since,
        read_with_if_unmodified_since,
        read_with_override_cache_control,
        read_with_override_content_disposition,
        read_with_override_content_type,
        read_with_version,
        write,
        write_can_multi,
        write_can_empty,
        write_can_append,
        write_with_content_type,
        write_with_content_disposition,
        write_with_content_encoding,
        write_with_cache_control,
        write_with_if_match,
        write_with_if_none_match,
        write_with_if_not_exists,
        write_with_user_metadata,
        write_multi_max_size,
        write_multi_min_size,
        write_total_max_size,
        create_dir,
        delete,
        delete_with_version,
        delete_max_size,
        copy,
        rename,
        list,
        list_with_limit,
        list_with_start_after,
        list_with_recursive,
        list_with_versions,
        list_with_deleted,
        list_has_cache_control,
        list_has_content_disposition,
        list_has_content_length,
        list_has_content_md5,
        list_has_content_range,
        list_has_content_type,
        list_has_etag,
        list_has_last_modified,
        list_has_version,
        list_has_user_metadata,
        presign,
        presign_read,
        presign_stat,
        presign_write,
        shared,
        blocking,
    });

    Ok(())
}
