// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! http_util contains the util types and functions that used across OpenDAL.
//!
//! # NOTE
//!
//! This mod is not a part of OpenDAL's public API. We expose them out to make
//! it easier to develop services and layers outside opendal.

mod client;
pub use client::HttpClient;

mod body;
pub use body::AsyncBody;
pub use body::Body;
pub use body::IncomingAsyncBody;

mod header;
pub use header::parse_content_length;
pub use header::parse_content_md5;
pub use header::parse_content_type;
pub use header::parse_etag;
pub use header::parse_last_modified;

mod uri;
pub use uri::percent_encode_path;

mod error;
pub use error::new_request_build_error;
pub use error::new_request_send_error;
pub use error::new_request_sign_error;
pub use error::new_response_consume_error;
pub use error::parse_error_response;
pub use error::ErrorResponse;
