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

use std::sync::Arc;

use bytes::Buf;

use super::core::HuggingfaceCore;
use super::core::HuggingfaceStatus;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct HuggingfaceLister {
    core: Arc<HuggingfaceCore>,
    path: String,
    recursive: bool,
}

impl HuggingfaceLister {
    pub fn new(core: Arc<HuggingfaceCore>, path: String, recursive: bool) -> Self {
        Self {
            core,
            path,
            recursive,
        }
    }
}

impl oio::PageList for HuggingfaceLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        // Use the next page URL from context if available, otherwise start from beginning
        let response = if ctx.token.is_empty() {
            self.core.hf_list(&self.path, self.recursive, None).await?
        } else {
            self.core.hf_list_with_url(&ctx.token).await?
        };

        let status_code = response.status();
        if !status_code.is_success() {
            let error = parse_error(response);
            return Err(error);
        }

        // Parse Link header for pagination
        let next_link = parse_link_header(response.headers());

        let bytes = response.into_body();
        let decoded_response: Vec<HuggingfaceStatus> =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        // Only mark as done if there's no next page
        if let Some(next_url) = next_link {
            ctx.token = next_url;
        } else {
            ctx.done = true;
        }

        for status in decoded_response {
            let entry_type = match status.type_.as_str() {
                "directory" => EntryMode::DIR,
                "file" => EntryMode::FILE,
                _ => EntryMode::Unknown,
            };

            let mut meta = Metadata::new(entry_type);

            if let Some(commit_info) = status.last_commit.as_ref() {
                meta.set_last_modified(commit_info.date.parse::<Timestamp>()?);
            }

            if entry_type == EntryMode::FILE {
                meta.set_content_length(status.size);

                // Use LFS OID as ETag if available, otherwise use regular OID
                let etag = if let Some(lfs) = &status.lfs {
                    &lfs.oid
                } else {
                    &status.oid
                };
                meta.set_etag(etag);
            }

            let path = if entry_type == EntryMode::DIR {
                format!("{}/", &status.path)
            } else {
                status.path.clone()
            };

            ctx.entries.push_back(oio::Entry::new(
                &build_rel_path(&self.core.root, &path),
                meta,
            ));
        }

        Ok(())
    }
}

/// Parse the Link header to extract the next page URL.
/// HuggingFace API returns pagination info in the Link header with rel="next".
/// Example: <https://huggingface.co/api/models/.../tree?cursor=xxx>; rel="next"
fn parse_link_header(headers: &http::HeaderMap) -> Option<String> {
    let link_header = headers.get(http::header::LINK)?;
    let link_str = link_header.to_str().ok()?;

    // Parse Link header format: <url>; rel="next"
    for link in link_str.split(',') {
        if link.contains("rel=\"next\"") || link.contains("rel='next'") {
            // Extract URL from <url> using split_once for cleaner parsing
            let (_, rest) = link.split_once('<')?;
            let (inside, _) = rest.split_once('>')?;
            return Some(inside.to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use http::HeaderValue;

    #[test]
    fn test_parse_link_header_with_next() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::LINK,
            HeaderValue::from_static(
                r#"<https://huggingface.co/api/models/test/tree/main?cursor=abc123>; rel="next""#,
            ),
        );

        let result = parse_link_header(&headers);
        assert_eq!(
            result,
            Some("https://huggingface.co/api/models/test/tree/main?cursor=abc123".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_with_single_quotes() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::LINK,
            HeaderValue::from_static(
                r#"<https://huggingface.co/api/models/test/tree/main?cursor=xyz>; rel='next'"#,
            ),
        );

        let result = parse_link_header(&headers);
        assert_eq!(
            result,
            Some("https://huggingface.co/api/models/test/tree/main?cursor=xyz".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_without_next() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::LINK,
            HeaderValue::from_static(
                r#"<https://huggingface.co/api/models/test/tree/main>; rel="prev""#,
            ),
        );

        let result = parse_link_header(&headers);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_link_header_multiple_links() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::LINK,
            HeaderValue::from_static(
                r#"<https://huggingface.co/api/prev>; rel="prev", <https://huggingface.co/api/next?cursor=456>; rel="next""#,
            ),
        );

        let result = parse_link_header(&headers);
        assert_eq!(
            result,
            Some("https://huggingface.co/api/next?cursor=456".to_string())
        );
    }

    #[test]
    fn test_parse_link_header_no_header() {
        let headers = HeaderMap::new();
        let result = parse_link_header(&headers);
        assert_eq!(result, None);
    }
}
