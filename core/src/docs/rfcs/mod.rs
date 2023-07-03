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

//! RFCs power OpenDAL's development.
//!
//! To add new features and big refactors:
//!
//! - Start a new RFCs with the template [`rfc_0000_example`].
//! - Submit PR and assign the RFC number with the PR number.
//! - Request reviews from OpenDAL maintainers.
//! - Create a tracking issue and update links in RFC after approval.
//!
//! Some useful tips:
//!
//! - Start a pre-propose in [discussion](https://github.com/apache/incubator-opendal/discussions/categories/ideas) to communicate quickly.
//! - The proposer of RFC may not be the same person as the implementor. Try to include enough information in RFC itself.

#[doc = "#"]
#[doc = include_str!("0000_example.md")]
pub mod rfc_0000_example {}

#[doc = "#"]
#[doc = include_str!("0041_object_native_api.md")]
pub mod rfc_0041_object_native_api {}

#[doc = "#"]
#[doc = include_str!("0044_error_handle.md")]
pub mod rfc_0044_error_handle {}

#[doc = "#"]
#[doc = include_str!("0057_auto_region.md")]
pub mod rfc_0057_auto_region {}

#[doc = "#"]
#[doc = include_str!("0069_object_stream.md")]
pub mod rfc_0069_object_stream {}

#[doc = "#"]
#[doc = include_str!("0090_limited_reader.md")]
pub mod rfc_0090_limited_reader {}

#[doc = "#"]
#[doc = include_str!("0112_path_normalization.md")]
pub mod rfc_0112_path_normalization {}

#[doc = "#"]
#[doc = include_str!("0191_async_streaming_io.md")]
pub mod rfc_0191_async_streaming_io {}

#[doc = "#"]
#[doc = include_str!("0203_remove_credential.md")]
pub mod rfc_0203_remove_credential {}

#[doc = "#"]
#[doc = include_str!("0221_create_dir.md")]
pub mod rfc_0221_create_dir {}

#[doc = "#"]
#[doc = include_str!("0247_retryable_error.md")]
pub mod rfc_0247_retryable_error {}

#[doc = "#"]
#[doc = include_str!("0293_object_id.md")]
pub mod rfc_0293_object_id {}

#[doc = "#"]
#[doc = include_str!("0337_dir_entry.md")]
pub mod rfc_0337_dir_entry {}

#[doc = "#"]
#[doc = include_str!("0409_accessor_capabilities.md")]
pub mod rfc_0409_accessor_capabilities {}

#[doc = "#"]
#[doc = include_str!("0413_presign.md")]
pub mod rfc_0413_presign {}

#[doc = "#"]
#[doc = include_str!("0423_command_line_interface.md")]
pub mod rfc_0423_command_line_interface {}

#[doc = "#"]
#[doc = include_str!("0429_init_from_iter.md")]
pub mod rfc_0429_init_from_iter {}

#[doc = "#"]
#[doc = include_str!("0438_multipart.md")]
pub mod rfc_0438_multipart {}

#[doc = "#"]
#[doc = include_str!("0443_gateway.md")]
pub mod rfc_0443_gateway {}

#[doc = "#"]
#[doc = include_str!("0501_new_builder.md")]
pub mod rfc_0501_new_builder {}

#[doc = "#"]
#[doc = include_str!("0554_write_refactor.md")]
pub mod rfc_0554_write_refactor {}

#[doc = "#"]
#[doc = include_str!("0561_list_metadata_reuse.md")]
pub mod rfc_0561_list_metadata_reuse {}

#[doc = "#"]
#[doc = include_str!("0599_blocking_api.md")]
pub mod rfc_0599_blocking_api {}

#[doc = "#"]
#[doc = include_str!("0623_redis_service.md")]
pub mod rfc_0623_redis_service {}

#[doc = "#"]
#[doc = include_str!("0627_split_capabilities.md")]
pub mod rfc_0627_split_capabilities {}

#[doc = "#"]
#[doc = include_str!("0661_path_in_accessor.md")]
pub mod rfc_0661_path_in_accessor {}

#[doc = "#"]
#[doc = include_str!("0793_generic_kv_services.md")]
pub mod rfc_0793_generic_kv_services {}

#[doc = "#"]
#[doc = include_str!("0926_object_reader.md")]
pub mod rfc_0926_object_reader {}

#[doc = "#"]
#[doc = include_str!("0977_refactor_error.md")]
pub mod rfc_0977_refactor_error {}

#[doc = "#"]
#[doc = include_str!("1085_object_handler.md")]
pub mod rfc_1085_object_handler {}

#[doc = "#"]
#[doc = include_str!("1391_object_metadataer.md")]
pub mod rfc_1391_object_metadataer {}

#[doc = "#"]
#[doc = include_str!("1398_query_based_metadata.md")]
pub mod rfc_1398_query_based_metadata {}

#[doc = "#"]
#[doc = include_str!("1420_object_writer.md")]
pub mod rfc_1420_object_writer {}

#[doc = "#"]
#[doc = include_str!("1477_remove_object_concept.md")]
pub mod rfc_1477_remove_object_concept {}

#[doc = "#"]
#[doc = include_str!("1735_operation_extension.md")]
pub mod rfc_1735_operation_extension {}

#[doc = "#"]
#[doc = include_str!("2083_writer_sink_api.md")]
pub mod rfc_2083_writer_sink_api {}

#[doc = "#"]
#[doc = include_str!("2133_append_api.md")]
pub mod rfc_2133_append_api {}

#[doc = "#"]
#[doc = include_str!("2299_chain_based_operator_api.md")]
pub mod rfc_2299_chain_based_operator_api {}
