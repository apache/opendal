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

use anyhow::anyhow;
use anyhow::Result;
use http::header::HeaderName;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_RANGE;
use http::header::CONTENT_TYPE;
use http::header::ETAG;
use http::header::LAST_MODIFIED;
use http::HeaderMap;
use time::format_description::well_known::Rfc2822;
use time::OffsetDateTime;

use crate::ops::BytesContentRange;

/// Parse content length from header map.
pub fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>> {
    match headers.get(CONTENT_LENGTH) {
        None => Ok(None),
        Some(v) => Ok(Some(
            v.to_str()
                .map_err(|e| anyhow!("parse {} header: {:?}", CONTENT_LENGTH, e))?
                .parse::<u64>()
                .map_err(|e| anyhow!("parse {} header: {:?}", CONTENT_LENGTH, e))?,
        )),
    }
}

/// Parse content md5 from header map.
pub fn parse_content_md5(headers: &HeaderMap) -> Result<Option<&str>> {
    match headers.get(HeaderName::from_static("content-md5")) {
        None => Ok(None),
        Some(v) => {
            Ok(Some(v.to_str().map_err(|e| {
                anyhow!("parse content-md5 header: {:?}", e)
            })?))
        }
    }
}

/// Parse content type from header map.
pub fn parse_content_type(headers: &HeaderMap) -> Result<Option<&str>> {
    match headers.get(CONTENT_TYPE) {
        None => Ok(None),
        Some(v) => {
            Ok(Some(v.to_str().map_err(|e| {
                anyhow!("parse content-type header: {:?}", e)
            })?))
        }
    }
}

/// Parse content range from header map.
pub fn parse_content_range(headers: &HeaderMap) -> Result<Option<BytesContentRange>> {
    match headers.get(CONTENT_RANGE) {
        None => Ok(None),
        Some(v) => Ok(Some(
            v.to_str()
                .map_err(|e| anyhow!("parse content-range header: {:?}", e))?
                .parse()?,
        )),
    }
}

/// Parse last modified from header map.
pub fn parse_last_modified(headers: &HeaderMap) -> Result<Option<OffsetDateTime>> {
    match headers.get(LAST_MODIFIED) {
        None => Ok(None),
        Some(v) => {
            let v = v
                .to_str()
                .map_err(|e| anyhow!("parse {} header: {:?}", LAST_MODIFIED, e))?;
            let t = OffsetDateTime::parse(v, &Rfc2822)
                .map_err(|e| anyhow!("parse {} header: {:?}", LAST_MODIFIED, e))?;

            Ok(Some(t))
        }
    }
}

/// Parse etag from header map.
pub fn parse_etag(headers: &HeaderMap) -> Result<Option<&str>> {
    match headers.get(ETAG) {
        None => Ok(None),
        Some(v) => Ok(Some(
            v.to_str()
                .map_err(|e| anyhow!("parse etag header: {:?}", e))?,
        )),
    }
}
