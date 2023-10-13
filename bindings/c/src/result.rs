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

use crate::error::opendal_error;
use crate::types::opendal_blocking_lister;
use crate::types::opendal_bytes;
use crate::types::opendal_metadata;
use crate::types::opendal_operator_ptr;

/// \brief The result type returned by opendal_operator_new() operation.
///
/// If the init logic is successful, the `operator_ptr` field will be set to a valid
/// pointer, and the `error` field will be set to null. If the init logic fails, the
/// `operator_ptr` field will be set to null, and the `error` field will be set to a
/// valid pointer with error code and error message.
///
/// @see opendal_operator_new()
/// @see opendal_operator_ptr
/// @see opendal_error
#[repr(C)]
pub struct opendal_result_operator_new {
    pub operator_ptr: *mut opendal_operator_ptr,
    pub error: *mut opendal_error,
}

/// \brief The result type returned by opendal's read operation.
///
/// The result type of read operation in opendal C binding, it contains
/// the data that the read operation returns and an NULL error.
/// If the read operation failed, the `data` fields should be a nullptr
/// and the error is not NULL.
#[repr(C)]
pub struct opendal_result_read {
    /// The byte array with length returned by read operations
    pub data: *mut opendal_bytes,
    /// The error, if ok, it is null
    pub error: *mut opendal_error,
}

/// \brief The result type returned by opendal_operator_is_exist().
///
/// The result type for opendal_operator_is_exist(), the field `is_exist`
/// contains whether the path exists, and the field `error` contains the
/// corresponding error. If successful, the `error` field is null.
///
/// \note If the opendal_operator_is_exist() fails, the `is_exist` field
/// will be set to false.
#[repr(C)]
pub struct opendal_result_is_exist {
    /// Whether the path exists
    pub is_exist: bool,
    /// The error, if ok, it is null
    pub error: *mut opendal_error,
}

/// \brief The result type returned by opendal_operator_stat().
///
/// The result type for opendal_operator_stat(), the field `meta` contains the metadata
/// of the path, the field `error` represents whether the stat operation is successful.
/// If successful, the `error` field is null.
#[repr(C)]
pub struct opendal_result_stat {
    /// The metadata output of the stat
    pub meta: *mut opendal_metadata,
    /// The error, if ok, it is null
    pub error: *mut opendal_error,
}

/// \brief The result type returned by opendal_operator_blocking_list().
///
/// The result type for opendal_operator_blocking_list(), the field `lister` contains the lister
/// of the path, which is an iterator of the objects under the path. the field `error` represents
/// whether the stat operation is successful. If successful, the `error` field is null.
#[repr(C)]
pub struct opendal_result_list {
    /// The lister, used for further listing operations
    pub lister: *mut opendal_blocking_lister,
    /// The error, if ok, it is null
    pub error: *mut opendal_error,
}
