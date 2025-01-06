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

#![warn(missing_docs)]
// This crate is the C binding for the OpenDAL project.
// So it's type node can't meet camel case.
#![allow(non_camel_case_types)]
// This crate is the C binding for the OpenDAL project.
// Nearly all the functions exposed to C FFI are unsafe.
#![allow(clippy::missing_safety_doc)]

//! The Apache OpenDAL C binding.
//!
//! The OpenDAL C binding allows users to utilize the OpenDAL's amazing storage accessing capability
//! in the C programming language.
//!
//! For examples, you may see the examples subdirectory

mod error;
pub use error::opendal_code;
pub use error::opendal_error;

mod lister;
pub use lister::opendal_lister;

mod metadata;
pub use metadata::opendal_metadata;

mod operator;
pub use operator::opendal_operator;

mod operator_info;

mod result;
pub use result::opendal_result_exists;
pub use result::opendal_result_is_exist;
pub use result::opendal_result_list;
pub use result::opendal_result_lister_next;
pub use result::opendal_result_operator_new;
pub use result::opendal_result_operator_reader;
pub use result::opendal_result_operator_writer;
pub use result::opendal_result_read;
pub use result::opendal_result_reader_read;
pub use result::opendal_result_stat;
pub use result::opendal_result_writer_write;

mod types;
pub use types::opendal_bytes;
pub use types::opendal_operator_options;

mod entry;
pub use entry::opendal_entry;

mod reader;
pub use reader::opendal_reader;

mod writer;
pub use writer::opendal_writer;
