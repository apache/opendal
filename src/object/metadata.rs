// Copyright 2022 Datafuse Labs
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

use flagset::flags;
use flagset::FlagSet;
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
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ObjectMetadata {
    /// bit stores current key store.
    bit: FlagSet<ObjectMetakey>,

    /// Mode of this object.
    mode: ObjectMode,

    /// Content-Disposition of this object
    content_disposition: Option<String>,
    /// Content Length of this object
    content_length: Option<u64>,
    /// Content MD5 of this object.
    content_md5: Option<String>,
    /// Content Range of this object.
    content_range: Option<BytesContentRange>,
    /// Content Type of this object.
    content_type: Option<String>,
    /// ETag of this object.
    etag: Option<String>,
    /// Last Modified of this object.
    last_modified: Option<OffsetDateTime>,
}

impl ObjectMetadata {
    /// Create a new object metadata
    pub fn new(mode: ObjectMode) -> Self {
        // Mode is required to be set for object metadata.
        let mut bit = ObjectMetakey::Mode.into();
        // If object mode is dir, we should always mark it as complete.
        if mode == ObjectMode::DIR {
            bit |= ObjectMetakey::Complete
        }

        Self {
            bit,
            mode,

            content_length: None,
            content_md5: None,
            content_type: None,
            content_range: None,
            last_modified: None,
            etag: None,
            content_disposition: None,
        }
    }

    /// Get the bit from object metadata.
    pub(crate) fn bit(&self) -> FlagSet<ObjectMetakey> {
        self.bit
    }

    /// Set bit with given.
    pub(crate) fn with_bit(mut self, bit: impl Into<FlagSet<ObjectMetakey>>) -> Self {
        self.bit = bit.into();
        self
    }

    /// Object mode represent this object's mode.
    pub fn mode(&self) -> ObjectMode {
        debug_assert!(
            self.bit.contains(ObjectMetakey::Mode) || self.bit.contains(ObjectMetakey::Complete),
            "visiting not set metadata: mode, maybe a bug"
        );

        self.mode
    }

    /// Set mode for object.
    pub fn set_mode(&mut self, mode: ObjectMode) -> &mut Self {
        self.mode = mode;
        self.bit |= ObjectMetakey::Mode;
        self
    }

    /// Set mode for object.
    pub fn with_mode(mut self, mode: ObjectMode) -> Self {
        self.mode = mode;
        self.bit |= ObjectMetakey::Mode;
        self
    }

    /// Content length of this object.
    ///
    /// `Content-Length` is defined by [RFC 7230](https://httpwg.org/specs/rfc7230.html#header.content-length)
    /// Refer to [MDN Content-Length](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Length) for more information.
    pub fn content_length(&self) -> u64 {
        debug_assert!(
            self.bit.contains(ObjectMetakey::ContentLength)
                || self.bit.contains(ObjectMetakey::Complete),
            "visiting not set metadata: content_length, maybe a bug"
        );

        self.content_length.unwrap_or_default()
    }

    /// Fetch the raw content length.
    pub(crate) fn content_length_raw(&self) -> Option<u64> {
        self.content_length
    }

    /// Set content length of this object.
    pub fn set_content_length(&mut self, content_length: u64) -> &mut Self {
        self.content_length = Some(content_length);
        self.bit |= ObjectMetakey::ContentLength;
        self
    }

    /// Set content length of this object.
    pub fn with_content_length(mut self, content_length: u64) -> Self {
        self.content_length = Some(content_length);
        self.bit |= ObjectMetakey::ContentLength;
        self
    }

    /// Content MD5 of this object.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    ///
    /// OpenDAL will try its best to set this value, but not guarantee this value is the md5 of content.
    pub fn content_md5(&self) -> Option<&str> {
        debug_assert!(
            self.bit.contains(ObjectMetakey::ContentMd5)
                || self.bit.contains(ObjectMetakey::Complete),
            "visiting not set metadata: content_md5, maybe a bug"
        );

        self.content_md5.as_deref()
    }

    /// Set content MD5 of this object.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    pub fn set_content_md5(&mut self, content_md5: &str) -> &mut Self {
        self.content_md5 = Some(content_md5.to_string());
        self.bit |= ObjectMetakey::ContentMd5;
        self
    }

    /// Set content MD5 of this object.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    pub fn with_content_md5(mut self, content_md5: String) -> Self {
        self.content_md5 = Some(content_md5);
        self.bit |= ObjectMetakey::ContentMd5;
        self
    }

    /// Content Type of this object.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    pub fn content_type(&self) -> Option<&str> {
        debug_assert!(
            self.bit.contains(ObjectMetakey::ContentType)
                || self.bit.contains(ObjectMetakey::Complete),
            "visiting not set metadata: content_type, maybe a bug"
        );

        self.content_type.as_deref()
    }

    /// Set Content Type of this object.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    pub fn set_content_type(&mut self, v: &str) -> &mut Self {
        self.content_type = Some(v.to_string());
        self.bit |= ObjectMetakey::ContentType;
        self
    }

    /// Set Content Type of this object.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    pub fn with_content_type(mut self, v: String) -> Self {
        self.content_type = Some(v);
        self.bit |= ObjectMetakey::ContentType;
        self
    }

    /// Content Range of this object.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    pub fn content_range(&self) -> Option<BytesContentRange> {
        debug_assert!(
            self.bit.contains(ObjectMetakey::ContentRange)
                || self.bit.contains(ObjectMetakey::Complete),
            "visiting not set metadata: content_range, maybe a bug"
        );

        self.content_range
    }

    /// Set Content Range of this object.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    pub fn set_content_range(&mut self, v: BytesContentRange) -> &mut Self {
        self.content_range = Some(v);
        self.bit |= ObjectMetakey::ContentRange;
        self
    }

    /// Set Content Range of this object.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    pub fn with_content_range(mut self, v: BytesContentRange) -> Self {
        self.content_range = Some(v);
        self.bit |= ObjectMetakey::ContentRange;
        self
    }

    /// Last modified of this object.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    ///
    /// OpenDAL parse the raw value into [`OffsetDateTime`] for convenient.
    pub fn last_modified(&self) -> Option<OffsetDateTime> {
        debug_assert!(
            self.bit.contains(ObjectMetakey::LastModified)
                || self.bit.contains(ObjectMetakey::Complete),
            "visiting not set metadata: last_modified, maybe a bug"
        );

        self.last_modified
    }

    /// Set Last modified of this object.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    pub fn set_last_modified(&mut self, last_modified: OffsetDateTime) -> &mut Self {
        self.last_modified = Some(last_modified);
        self.bit |= ObjectMetakey::LastModified;
        self
    }

    /// Set Last modified of this object.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    pub fn with_last_modified(mut self, last_modified: OffsetDateTime) -> Self {
        self.last_modified = Some(last_modified);
        self.bit |= ObjectMetakey::LastModified;
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
        debug_assert!(
            self.bit.contains(ObjectMetakey::Etag) || self.bit.contains(ObjectMetakey::Complete),
            "visiting not set metadata: etag, maybe a bug"
        );

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
        self.bit |= ObjectMetakey::Etag;
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
    pub fn with_etag(mut self, etag: String) -> Self {
        self.etag = Some(etag);
        self.bit |= ObjectMetakey::Etag;
        self
    }

    /// Content-Disposition of this object
    ///
    /// `Content-Disposition` is defined by [RFC 2616](https://www.rfc-editor/rfcs/2616) and
    /// clarified usage in [RFC 6266](https://www.rfc-editor/6266).
    /// Refer to [MDN Content-Disposition](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition) for more information.
    ///
    /// OpenDAL will return this value AS-IS like the following:
    ///
    /// - "inline"
    /// - "attachment"
    /// - "attachment; filename=\"filename.jpg\""
    pub fn content_disposition(&self) -> Option<&str> {
        debug_assert!(
            self.bit.contains(ObjectMetakey::ContentDisposition)
                || self.bit.contains(ObjectMetakey::Complete),
            "visiting not set metadata: content_disposition, maybe a bug"
        );

        self.content_disposition.as_deref()
    }

    /// Set Content-Disposition of this object
    ///
    /// `Content-Disposition` is defined by [RFC 2616](https://www.rfc-editor/rfcs/2616) and
    /// clarified usage in [RFC 6266](https://www.rfc-editor/6266).
    /// Refer to [MDN Content-Disposition](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition) for more information.
    ///
    /// OpenDAL will return this value AS-IS like the following:
    ///
    /// - "inline"
    /// - "attachment"
    /// - "attachment; filename=\"filename.jpg\""
    pub fn with_content_disposition(mut self, content_disposition: String) -> Self {
        self.content_disposition = Some(content_disposition);
        self.bit |= ObjectMetakey::ContentDisposition;
        self
    }

    /// Set Content-Disposition of this object
    ///
    /// `Content-Disposition` is defined by [RFC 2616](https://www.rfc-editor/rfcs/2616) and
    /// clarified usage in [RFC 6266](https://www.rfc-editor/6266).
    /// Refer to [MDN Content-Disposition](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition) for more information.
    ///
    /// OpenDAL will return this value AS-IS like the following:
    ///
    /// - "inline"
    /// - "attachment"
    /// - "attachment; filename=\"filename.jpg\""
    pub fn set_content_disposition(&mut self, content_disposition: &str) -> &mut Self {
        self.content_disposition = Some(content_disposition.to_string());
        self.bit |= ObjectMetakey::ContentDisposition;
        self
    }
}

flags! {
    /// ObjectMetakey describes the metadata keys that can be stored
    /// or queried.
    ///
    /// ## For store
    ///
    /// Internally, we will store a flag set of ObjectMetakey to check
    /// whether we have set some key already.
    ///
    /// ## For query
    ///
    /// At user side, we will allow user to query the object metadata. If
    /// the meta has been stored, we will return directly. If no, we will
    /// call `stat` internally to fecth the metadata.
    pub enum ObjectMetakey: u64 {
        /// The special object metadata key that used to mark this object
        /// already contains all metadata.
        Complete,

        /// Key for mode.
        Mode,
        /// Key for content disposition.
        ContentDisposition,
        /// Key for content length.
        ContentLength,
        /// Key for content md5.
        ContentMd5,
        /// Key for content range.
        ContentRange,
        /// Key for content type.
        ContentType,
        /// Key for etag.
        Etag,
        /// Key for last last modified.
        LastModified,
    }
}
