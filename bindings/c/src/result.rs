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
use crate::types::opendal_blocking_lister;
use crate::types::opendal_bytes;
use crate::types::opendal_metadata;

/// \brief The result type returned by opendal's read operation.
///
/// The result type of read operation in opendal C binding, it contains
/// the data that the read operation returns and a error code.
/// If the read operation failed, the `data` fields should be a nullptr
/// and the error code is **NOT** OPENDAL_OK.
#[repr(C)]
pub struct opendal_result_read {
    /// The byte array with length returned by read operations
    pub data: *mut opendal_bytes,
    /// The error code, should be OPENDAL_OK if succeeds
    pub code: opendal_code,
}

/// \brief The result type returned by opendal_operator_is_exist().
///
/// The result type for opendal_operator_is_exist(), the field `is_exist`
/// contains whether the path exists, and the field `code` contains the
/// corresponding error code.
///
/// \note If the opendal_operator_is_exist() fails, the `is_exist` field
/// will be set to false.
#[repr(C)]
pub struct opendal_result_is_exist {
    /// Whether the path exists
    pub is_exist: bool,
    /// The error code, should be OPENDAL_OK if succeeds
    pub code: opendal_code,
}

/// \brief The result type returned by opendal_operator_stat().
///
/// The result type for opendal_operator_stat(), the field `meta` contains the metadata
/// of the path, the field `code` represents whether the stat operation is successful.
#[repr(C)]
pub struct opendal_result_stat {
    /// The metadata output of the stat
    pub meta: *mut opendal_metadata,
    /// The error code, should be OPENDAL_OK if succeeds
    pub code: opendal_code,
}

/// \brief The result type returned by opendal_operator_blocking_list().
///
/// The result type for opendal_operator_blocking_list(), the field `lister` contains the lister
/// of the path, which is an iterator of the objects under the path. the field `code` represents
/// whether the stat operation is successful.
#[repr(C)]
pub struct opendal_result_list {
    pub lister: *mut opendal_blocking_lister,
    pub code: opendal_code,
}
