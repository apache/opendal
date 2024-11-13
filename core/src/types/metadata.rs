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

use std::collections::HashMap;

use chrono::prelude::*;
use flagset::flags;
use flagset::FlagSet;

use crate::raw::*;
use crate::*;

/// Metadata carries all metadata associated with a path.
///
/// # Notes
///
/// mode and content_length are required metadata that all services
/// should provide during `stat` operation. But in `list` operation,
/// a.k.a., `Entry`'s content length could be `None`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Metadata {
    /// metakey stores current key store.
    metakey: FlagSet<Metakey>,

    mode: EntryMode,

    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_length: Option<u64>,
    content_md5: Option<String>,
    content_range: Option<BytesContentRange>,
    content_type: Option<String>,
    etag: Option<String>,
    last_modified: Option<DateTime<Utc>>,
    version: Option<String>,

    user_metadata: Option<HashMap<String, String>>,
}

impl Metadata {
    /// Create a new metadata
    pub fn new(mode: EntryMode) -> Self {
        // Mode is required to be set for metadata.
        let mut metakey: FlagSet<Metakey> = Metakey::Mode.into();
        // If mode is dir, we should always mark it as complete.
        if mode.is_dir() {
            metakey |= Metakey::Complete
        }

        Self {
            metakey,

            mode,

            cache_control: None,
            content_length: None,
            content_md5: None,
            content_type: None,
            content_range: None,
            last_modified: None,
            etag: None,
            content_disposition: None,
            version: None,
            user_metadata: None,
        }
    }

    /// Get the metakey from metadata.
    ///
    /// This value describes which metadata has been set.
    pub fn metakey(&self) -> FlagSet<Metakey> {
        self.metakey
    }

    /// Set metakey with given.
    pub(crate) fn with_metakey(mut self, metakey: impl Into<FlagSet<Metakey>>) -> Self {
        self.metakey = metakey.into();
        self
    }

    /// Check if the metadata already contains given metakey.
    pub(crate) fn contains_metakey(&self, metakey: impl Into<FlagSet<Metakey>>) -> bool {
        let input_metakey = metakey.into();

        // If meta already contains complete, we don't need to check.
        if self.metakey.contains(Metakey::Complete) {
            return true;
        }

        self.metakey.contains(input_metakey)
    }

    /// mode represent this entry's mode.
    pub fn mode(&self) -> EntryMode {
        debug_assert!(
            self.metakey.contains(Metakey::Mode) || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: mode, maybe a bug"
        );

        self.mode
    }

    /// Returns `true` if this metadata is for a file.
    pub fn is_file(&self) -> bool {
        matches!(self.mode, EntryMode::FILE)
    }

    /// Returns `true` if this metadata is for a directory.
    pub fn is_dir(&self) -> bool {
        matches!(self.mode, EntryMode::DIR)
    }

    /// Set mode for entry.
    pub fn set_mode(&mut self, v: EntryMode) -> &mut Self {
        self.mode = v;
        self.metakey |= Metakey::Mode;
        self
    }

    /// Set mode for entry.
    pub fn with_mode(mut self, v: EntryMode) -> Self {
        self.mode = v;
        self.metakey |= Metakey::Mode;
        self
    }

    /// Cache control of this entry.
    /// Cache-Control is defined by [RFC 7234](https://httpwg.org/specs/rfc7234.html#header.cache-control)
    /// Refer to [MDN Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) for more information.
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::CacheControl`], otherwise this method returns `None`.
    pub fn cache_control(&self) -> Option<&str> {
        #[cfg(feature = "tests")]
        debug_assert!(
            self.metakey.contains(Metakey::CacheControl)
                || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: cache_control, maybe a bug"
        );

        self.cache_control.as_deref()
    }

    /// Set cache control of this entry.
    ///
    /// Cache-Control is defined by [RFC 7234](https://httpwg.org/specs/rfc7234.html#header.cache-control)
    /// Refer to [MDN Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) for more information.
    pub fn set_cache_control(&mut self, v: &str) -> &mut Self {
        self.cache_control = Some(v.to_string());
        self.metakey |= Metakey::CacheControl;
        self
    }

    /// Set cache control of this entry.
    ///
    /// Cache-Control is defined by [RFC 7234](https://httpwg.org/specs/rfc7234.html#header.cache-control)
    /// Refer to [MDN Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) for more information.
    pub fn with_cache_control(mut self, v: String) -> Self {
        self.cache_control = Some(v);
        self.metakey |= Metakey::CacheControl;
        self
    }

    /// Content length of this entry.
    ///
    /// `Content-Length` is defined by [RFC 7230](https://httpwg.org/specs/rfc7230.html#header.content-length)
    /// Refer to [MDN Content-Length](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Length) for more information.
    ///
    /// # Panics
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::ContentLength`], otherwise it will panic.
    pub fn content_length(&self) -> u64 {
        debug_assert!(
            self.metakey.contains(Metakey::ContentLength)
                || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: content_length, maybe a bug"
        );

        self.content_length.unwrap_or_default()
    }

    /// Set content length of this entry.
    pub fn set_content_length(&mut self, v: u64) -> &mut Self {
        self.content_length = Some(v);
        self.metakey |= Metakey::ContentLength;
        self
    }

    /// Set content length of this entry.
    pub fn with_content_length(mut self, v: u64) -> Self {
        self.content_length = Some(v);
        self.metakey |= Metakey::ContentLength;
        self
    }

    /// Content MD5 of this entry.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    ///
    /// OpenDAL will try its best to set this value, but not guarantee this value is the md5 of content.
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::ContentMd5`], otherwise this method returns `None`.
    pub fn content_md5(&self) -> Option<&str> {
        #[cfg(feature = "tests")]
        debug_assert!(
            self.metakey.contains(Metakey::ContentMd5) || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: content_md5, maybe a bug"
        );

        self.content_md5.as_deref()
    }

    /// Set content MD5 of this entry.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    pub fn set_content_md5(&mut self, v: &str) -> &mut Self {
        self.content_md5 = Some(v.to_string());
        self.metakey |= Metakey::ContentMd5;
        self
    }

    /// Set content MD5 of this entry.
    ///
    /// Content MD5 is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    pub fn with_content_md5(mut self, v: String) -> Self {
        self.content_md5 = Some(v);
        self.metakey |= Metakey::ContentMd5;
        self
    }

    /// Content Type of this entry.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::ContentType`], otherwise this method returns `None`.
    pub fn content_type(&self) -> Option<&str> {
        #[cfg(feature = "tests")]
        debug_assert!(
            self.metakey.contains(Metakey::ContentType) || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: content_type, maybe a bug"
        );

        self.content_type.as_deref()
    }

    /// Set Content Type of this entry.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    pub fn set_content_type(&mut self, v: &str) -> &mut Self {
        self.content_type = Some(v.to_string());
        self.metakey |= Metakey::ContentType;
        self
    }

    /// Set Content Type of this entry.
    ///
    /// Content Type is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-type).
    pub fn with_content_type(mut self, v: String) -> Self {
        self.content_type = Some(v);
        self.metakey |= Metakey::ContentType;
        self
    }

    /// Content Range of this entry.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::ContentRange`], otherwise this method returns `None`.
    pub fn content_range(&self) -> Option<BytesContentRange> {
        #[cfg(feature = "tests")]
        debug_assert!(
            self.metakey.contains(Metakey::ContentRange)
                || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: content_range, maybe a bug"
        );

        self.content_range
    }

    /// Set Content Range of this entry.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    pub fn set_content_range(&mut self, v: BytesContentRange) -> &mut Self {
        self.content_range = Some(v);
        self.metakey |= Metakey::ContentRange;
        self
    }

    /// Set Content Range of this entry.
    ///
    /// Content Range is defined by [RFC 9110](https://httpwg.org/specs/rfc9110.html#field.content-range).
    pub fn with_content_range(mut self, v: BytesContentRange) -> Self {
        self.content_range = Some(v);
        self.metakey |= Metakey::ContentRange;
        self
    }

    /// Last modified of this entry.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    ///
    /// OpenDAL parse the raw value into [`DateTime`] for convenient.
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::LastModified`], otherwise this method returns `None`.
    pub fn last_modified(&self) -> Option<DateTime<Utc>> {
        #[cfg(feature = "tests")]
        debug_assert!(
            self.metakey.contains(Metakey::LastModified)
                || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: last_modified, maybe a bug"
        );

        self.last_modified
    }

    /// Set Last modified of this entry.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    pub fn set_last_modified(&mut self, v: DateTime<Utc>) -> &mut Self {
        self.last_modified = Some(v);
        self.metakey |= Metakey::LastModified;
        self
    }

    /// Set Last modified of this entry.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    pub fn with_last_modified(mut self, v: DateTime<Utc>) -> Self {
        self.last_modified = Some(v);
        self.metakey |= Metakey::LastModified;
        self
    }

    /// ETag of this entry.
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
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::Etag`], otherwise this method returns `None`.
    pub fn etag(&self) -> Option<&str> {
        #[cfg(feature = "tests")]
        debug_assert!(
            self.metakey.contains(Metakey::Etag) || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: etag, maybe a bug"
        );

        self.etag.as_deref()
    }

    /// Set ETag of this entry.
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
    pub fn set_etag(&mut self, v: &str) -> &mut Self {
        self.etag = Some(v.to_string());
        self.metakey |= Metakey::Etag;
        self
    }

    /// Set ETag of this entry.
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
    pub fn with_etag(mut self, v: String) -> Self {
        self.etag = Some(v);
        self.metakey |= Metakey::Etag;
        self
    }

    /// Content-Disposition of this entry
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
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::ContentDisposition`], otherwise this method returns `None`.
    pub fn content_disposition(&self) -> Option<&str> {
        #[cfg(feature = "tests")]
        debug_assert!(
            self.metakey.contains(Metakey::ContentDisposition)
                || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: content_disposition, maybe a bug"
        );

        self.content_disposition.as_deref()
    }

    /// Set Content-Disposition of this entry
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
    pub fn with_content_disposition(mut self, v: String) -> Self {
        self.content_disposition = Some(v);
        self.metakey |= Metakey::ContentDisposition;
        self
    }

    /// Set Content-Disposition of this entry
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
    pub fn set_content_disposition(&mut self, v: &str) -> &mut Self {
        self.content_disposition = Some(v.to_string());
        self.metakey |= Metakey::ContentDisposition;
        self
    }

    /// Version of this entry.
    ///
    /// Version is a string that can be used to identify the version of this entry.
    ///
    /// This field may come out from the version control system, like object versioning in AWS S3.
    ///
    /// This value is only available when calling on result of `stat` or `list` with
    /// [`Metakey::Version`], otherwise this method returns `None`.
    pub fn version(&self) -> Option<&str> {
        #[cfg(feature = "tests")]
        debug_assert!(
            self.metakey.contains(Metakey::Version) || self.metakey.contains(Metakey::Complete),
            "visiting not set metadata: version, maybe a bug"
        );

        self.version.as_deref()
    }

    /// Set version of this entry.
    ///
    /// Version is a string that can be used to identify the version of this entry.
    ///
    /// This field may come out from the version control system, like object versioning in AWS S3.
    pub fn with_version(mut self, v: String) -> Self {
        self.version = Some(v);
        self.metakey |= Metakey::Version;
        self
    }

    /// Set version of this entry.
    ///
    /// Version is a string that can be used to identify the version of this entry.
    ///
    /// This field may come out from the version control system, like object versioning in AWS S3.
    pub fn set_version(&mut self, v: &str) -> &mut Self {
        self.version = Some(v.to_string());
        self.metakey |= Metakey::Version;
        self
    }

    /// User defined metadata of this entry
    ///
    /// The prefix of the user defined metadata key(for example: in oss, it's x-oss-meta-)
    /// is remove from the key
    pub fn user_metadata(&self) -> Option<&HashMap<String, String>> {
        self.user_metadata.as_ref()
    }

    /// Set user defined metadata of this entry
    pub fn with_user_metadata(&mut self, data: HashMap<String, String>) -> &mut Self {
        self.user_metadata = Some(data);
        self.metakey |= Metakey::UserMetaData;
        self
    }
}

flags! {
    /// Metakey describes the metadata keys that can be stored
    /// or queried.
    ///
    /// ## For store
    ///
    /// Internally, we will store a flag set of Metakey to check
    /// whether we have set some key already.
    ///
    /// ## For query
    ///
    /// At user side, we will allow user to query the metadata. If
    /// the meta has been stored, we will return directly. If no, we will
    /// call `stat` internally to fetch the metadata.
    pub enum Metakey: u64 {
        /// The special metadata key that used to mark this entry
        /// already contains all metadata.
        Complete,

        /// Key for mode.
        Mode,
        /// Key for cache control.
        CacheControl,
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
        /// Key for last modified.
        LastModified,
        /// Key for version.
        Version,
        /// Key for user metadata
        UserMetaData,
    }
}
