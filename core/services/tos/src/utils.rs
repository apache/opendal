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

use http::HeaderMap;
use http::header::ETAG;
use opendal_core::Metadata;
use opendal_core::raw::{parse_header_to_str, parse_into_metadata};

/// Parse etag from header map.
///
/// Trim quotes from etag if present (TOS returns etag with quotes)
pub fn tos_parse_etag(headers: &HeaderMap) -> opendal_core::Result<Option<&str>> {
    let etag = parse_header_to_str(headers, ETAG)?.map(|etag| etag.trim_matches('"'));
    Ok(etag)
}

pub(crate) fn tos_parse_into_metadata(
    path: &str,
    headers: &HeaderMap,
) -> opendal_core::Result<Metadata> {
    let mut metadata = parse_into_metadata(path, headers)?;

    if let Some(etag) = tos_parse_etag(headers)? {
        metadata.set_etag(etag);
    }

    Ok(metadata)
}
