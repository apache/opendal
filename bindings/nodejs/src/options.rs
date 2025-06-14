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

use opendal::raw::parse_datetime_from_rfc3339;

#[napi(object)]
#[derive(Debug)]
pub struct StatOptions {
    /**
     * Sets version for this operation.
     * Retrieves data of a specified version of the given path.
     */
    pub version: Option<String>,

    /**
     * Sets if-match condition for this operation.
     * If file exists and its etag doesn't match, an error will be returned.
     */
    pub if_match: Option<String>,

    /**
     * Sets if-none-match condition for this operation.
     * If file exists and its etag matches, an error will be returned.
     */
    pub if_none_match: Option<String>,

    /**
     * Sets if-modified-since condition for this operation.
     * If file exists and hasn't been modified since the specified time, an error will be returned.
     * ISO 8601 formatted date string
     * https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString
     */
    pub if_modified_since: Option<String>,

    /**
     * Sets if-unmodified-since condition for this operation.
     * If file exists and has been modified since the specified time, an error will be returned.
     * ISO 8601 formatted date string
     * https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString
     */
    pub if_unmodified_since: Option<String>,

    /**
     * Specifies the content-type header for presigned operations.
     * Only meaningful when used along with presign.
     */
    pub override_content_type: Option<String>,

    /**
     * Specifies the cache-control header for presigned operations.
     * Only meaningful when used along with presign.
     */
    pub override_cache_control: Option<String>,

    /**
     * Specifies the content-disposition header for presigned operations.
     * Only meaningful when used along with presign.
     */
    pub override_content_disposition: Option<String>,
}

impl From<StatOptions> for opendal::options::StatOptions {
    fn from(value: StatOptions) -> Self {
        let if_modified_since = value
            .if_modified_since
            .and_then(|v| parse_datetime_from_rfc3339(&v).ok());
        let if_unmodified_since = value
            .if_unmodified_since
            .and_then(|v| parse_datetime_from_rfc3339(&v).ok());

        Self {
            if_modified_since,
            if_unmodified_since,
            version: value.version,
            if_match: value.if_match,
            if_none_match: value.if_none_match,
            override_content_type: value.override_content_type,
            override_cache_control: value.override_cache_control,
            override_content_disposition: value.override_content_disposition,
        }
    }
}
