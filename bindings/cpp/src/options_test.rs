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

//! The generated `ListOptions` conversion in `options.rs` carries every bridge
//! field to the core field of the same name, and absent values stay absent.

use opendal::options::ListOptions;

use crate::options::ffi;

fn bridge_list_options(limit: Option<usize>, start_after: Option<&str>) -> ffi::FfiListOptions {
    ffi::FfiListOptions {
        limit: ffi::OptionalUsize {
            has_value: limit.is_some(),
            value: limit.unwrap_or(7),
        },
        start_after: ffi::OptionalString {
            has_value: start_after.is_some(),
            value: start_after.unwrap_or("ignored").to_string(),
        },
        recursive: false,
        versions: false,
        deleted: false,
    }
}

#[test]
fn test_list_options_carry_every_field() {
    let opts = ffi::FfiListOptions {
        recursive: true,
        versions: true,
        deleted: true,
        ..bridge_list_options(Some(16), Some("start_after"))
    };

    let expected = ListOptions {
        limit: Some(16),
        start_after: Some("start_after".to_string()),
        recursive: true,
        versions: true,
        deleted: true,
        ..Default::default()
    };
    assert_eq!(ListOptions::from(opts), expected);
}

#[test]
fn test_list_options_keep_explicit_zero_limit() {
    let opts = bridge_list_options(Some(0), None);

    assert_eq!(ListOptions::from(opts).limit, Some(0));
}

#[test]
fn test_list_options_keep_absent_values_absent() {
    // C++ sends a value behind `has_value == false`; it must not reach core.
    let opts = bridge_list_options(None, None);

    assert_eq!(ListOptions::from(opts), ListOptions::default());
}
