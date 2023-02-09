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

use time::OffsetDateTime;

use crate::raw::*;
use crate::*;

/// Metadata carries all object metadata.
///
/// # Notes
///
/// mode and content_length are required metadata that all services
/// should provide during `stat` operation. But in `list` operation,
/// a.k.a., `Entry`'s content length could be `None`.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ObjectMetadata {
    /// Mark if this metadata is complete or not.
    complete: bool,

    /// Mode of this object.
    mode: ObjectMode,

    /// Content Length of this object
    ///
    /// # NOTE
    ///
    /// - For `stat` operation, content_length is required to set.
    /// - For `list` operation, content_length could be None.
    /// - For `read` operation, content_length could be the length of request.
    content_length: Option<u64>,
    /// Content MD5 of this object.
    content_md5: Option<String>,
    /// Content Type of this object.
    content_type: Option<String>,
    /// Content Range of this object.
    content_range: Option<BytesContentRange>,
    /// Last Modified of this object.
    last_modified: Option<OffsetDateTime>,
    /// ETag of this object.
    etag: Option<String>,
}

impl ObjectMetadata {
    /// Create a new object metadata
    pub fn new(mode: ObjectMode) -> Self {
        Self {
            complete: false,

            mode,

            content_length: None,
            content_md5: None,
            content_type: None,
            content_range: None,
            last_modified: None,
            etag: None,
        }
    }

    /// If this object metadata if complete
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Make this object metadata if complete.
    pub fn set_complete(&mut self) -> &mut Self {
        self.complete = true;
        self
    }

    /// Make this object metadata if complete.
    pub fn with_complete(mut self) -> Self {
        self.complete = true;
        self
    }

    /// Object mode represent this object's mode.
    pub fn mode(&self) -> ObjectMode {
        self.mode
    }

    /// Set mode for object.
    pub fn set_mode(&mut self, mode: ObjectMode) -> &mut Self {
        self.mode = mode;
        self
    }

    /// Set mode for object.
    pub fn with_mode(mut self, mode: ObjectMode) -> Self {
        self.mode = mode;
        self
    }

    /// Content length of this object.
    ///
    /// `Content-Length` is defined by [RFC 7230](https://httpwg.org/specs/rfc7230.html#header.content-length)
    /// Refer to [MDN Content-Length](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Length) for more information.
    pub fn content_length(&self) -> u64 {
        self.content_length.unwrap_or_default()
    }

    /// Fetch the raw content length.
    pub(crate) fn content_length_raw(&self) -> Option<u64> {
        self.content_length
    }

    /// Set content length of this object.
    pub fn set_content_length(&mut self, content_length: u64) -> &mut Self {
        self.content_length = Some(content_length);
        self
    }

    /// Set content length of this object.
    pub fn with_content_length(mut self, content_length: u64) -> Self {
        self.content_length = Some(content_length);
        self
    }

    /// Content MD5 of this object.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    ///
    /// OpenDAL will try its best to set this value, but not guarantee this value is the md5 of content.
    pub fn content_md5(&self) -> Option<&str> {
        self.content_md5.as_deref()
    }

    /// Set content MD5 of this object.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    pub fn set_content_md5(&mut self, content_md5: &str) -> &mut Self {
        self.content_md5 = Some(content_md5.to_string());
        self
    }

    /// Set content MD5 of this object.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    pub fn with_content_md5(mut self, content_md5: &str) -> Self {
        self.content_md5 = Some(content_md5.to_string());
        self
    }

    /// Content Type of this object.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// Set Content Type of this object.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    pub fn set_content_type(&mut self, v: &str) -> &mut Self {
        self.content_type = Some(v.to_string());
        self
    }

    /// Set Content Type of this object.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    pub fn with_content_type(mut self, v: &str) -> Self {
        self.content_type = Some(v.to_string());
        self
    }

    /// Content Range of this object.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    pub fn content_range(&self) -> Option<BytesContentRange> {
        self.content_range
    }

    /// Set Content Range of this object.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    pub fn set_content_range(&mut self, v: BytesContentRange) -> &mut Self {
        self.content_range = Some(v);
        self
    }

    /// Set Content Range of this object.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    pub fn with_content_range(mut self, v: BytesContentRange) -> Self {
        self.content_range = Some(v);
        self
    }

    /// Last modified of this object.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    ///
    /// OpenDAL parse the raw value into [`OffsetDateTime`] for convenient.
    pub fn last_modified(&self) -> Option<OffsetDateTime> {
        self.last_modified
    }

    /// Set Last modified of this object.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    pub fn set_last_modified(&mut self, last_modified: OffsetDateTime) -> &mut Self {
        self.last_modified = Some(last_modified);
        self
    }

    /// Set Last modified of this object.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    pub fn with_last_modified(mut self, last_modified: OffsetDateTime) -> Self {
        self.last_modified = Some(last_modified);
        self
    }

    /// ETag of this object.
    ///
    /// `ETag` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.etag)
    /// Refer to [MDN ETag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) for more information.
    ///
    /// OpenDAL will return this value AS-IS like the following:
    ///
    /// - `"33a64df551425fcc55e4d42a148795d9f25f89d4"`
    /// - `W/"0815"`
    ///
    /// `"` is part of etag.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Set ETag of this object.
    ///
    /// `ETag` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.etag)
    /// Refer to [MDN ETag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) for more information.
    ///
    /// OpenDAL will return this value AS-IS like the following:
    ///
    /// - `"33a64df551425fcc55e4d42a148795d9f25f89d4"`
    /// - `W/"0815"`
    ///
    /// `"` is part of etag, don't trim it before setting.
    pub fn set_etag(&mut self, etag: &str) -> &mut Self {
        self.etag = Some(etag.to_string());
        self
    }

    /// Set ETag of this object.
    ///
    /// `ETag` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.etag)
    /// Refer to [MDN ETag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) for more information.
    ///
    /// OpenDAL will return this value AS-IS like the following:
    ///
    /// - `"33a64df551425fcc55e4d42a148795d9f25f89d4"`
    /// - `W/"0815"`
    ///
    /// `"` is part of etag, don't trim it before setting.
    pub fn with_etag(mut self, etag: &str) -> Self {
        self.etag = Some(etag.to_string());
        self
    }
}
