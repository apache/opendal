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

//! This is for better naming in C header file. If we use generics for Result type,
//! it will no doubt work find. However, the generics will lead to naming like
//! "opendal_result_opendal_operator_ptr", which is unacceptable. Therefore,
//! we are defining all Result types here

use crate::error::opendal_code;
use crate::types::opendal_bytes;
use crate::types::opendal_metadata;

/// The Rust-like Result type of opendal C binding, it contains
/// the data that the read operation returns and a error code
/// If the read operation failed, the `data` fields should be a nullptr
/// and the error code is NOT OPENDAL_OK.
#[repr(C)]
pub struct opendal_result_read {
    pub data: *mut opendal_bytes,
    pub code: opendal_code,
}

/// The result type for [`opendal_operator_is_exist()`], the field `is_exist`
/// contains whether the path exists, and the field `code` contains the
/// corresponding error code.
#[repr(C)]
pub struct opendal_result_is_exist {
    pub is_exist: bool,
    pub code: opendal_code,
}

/// The result type for [`opendal_operator_stat()`], the meta contains the metadata
/// of the path, the code represents whether the stat operation is successful. Note
/// that the operation could be successful even if the path does not exist.
#[repr(C)]
pub struct opendal_result_stat {
    pub meta: opendal_metadata,
    pub code: opendal_code,
}
