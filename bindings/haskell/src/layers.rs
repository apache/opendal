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

use std::ffi::c_char;

use ::opendal as od;

#[repr(C)]
pub struct Layer {
    tag: LayerTag,
    data: LayerData,
}

#[allow(dead_code)]
#[repr(C)]
pub enum LayerTag {
    ConcurrentLimit,
    ImmutableIndex,
}

#[repr(C)]
pub union LayerData {
    concurrent_limit: ConcurrentLimit,
    immutable_index: ImmutableIndex,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ConcurrentLimit {
    permits: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ImmutableIndex {
    keys: *const *const c_char,
    keys_len: usize,
}

pub fn apply_layer(operator: od::Operator, layer: &Layer) -> od::Operator {
    match layer.tag {
        LayerTag::ConcurrentLimit => {
            let layer = unsafe { &layer.data.concurrent_limit };
            operator.layer(od::layers::ConcurrentLimitLayer::new(layer.permits))
        }
        LayerTag::ImmutableIndex => {
            let layer = unsafe { &layer.data.immutable_index };
            let key_slice = unsafe { std::slice::from_raw_parts(layer.keys, layer.keys_len) };
            let iter = key_slice.iter().map(|key| unsafe {
                std::ffi::CStr::from_ptr(*key)
                    .to_string_lossy()
                    .into_owned()
            });

            let mut layer = od::layers::ImmutableIndexLayer::default();
            layer.extend_iter(iter);
            operator.layer(layer)
        }
    }
}
