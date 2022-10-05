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

use bincode::de::Decoder;
use bincode::enc::Encoder;
use bincode::error::{DecodeError, EncodeError};
use std::time::SystemTime;
use time::{OffsetDateTime, UtcOffset};

use crate::ObjectMode;

/// Metadata carries all object metadata.
///
/// # Notes
///
/// mode and content_length are required metadata that all services
/// should provide during `stat` operation. But in `list` operation,
/// a.k.a., `ObjectEntry`'s content length could be `None`.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ObjectMetadata {
    mode: ObjectMode,

    /// # NOTE
    ///
    /// - For `stat` operation, content_length is required to set.
    /// - For `list` operation, content_length could be None.
    content_length: Option<u64>,
    content_md5: Option<String>,
    /// # NOTE
    ///
    /// bincode::{Encode, Decode} is not implemented on OffsetDateTime.
    /// We will convert it to (SystemTime, (h,m,s)) instead.
    last_modified: Option<OffsetDateTime>,
    etag: Option<String>,
}

impl bincode::Encode for ObjectMetadata {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        bincode::Encode::encode(&self.mode, encoder)?;
        bincode::Encode::encode(&self.content_length, encoder)?;
        bincode::Encode::encode(&self.content_md5, encoder)?;
        bincode::Encode::encode(
            &self.last_modified.map(|v| {
                (
                    SystemTime::from(v.to_offset(UtcOffset::UTC)),
                    v.offset().as_hms(),
                )
            }),
            encoder,
        )?;
        bincode::Encode::encode(&self.etag, encoder)?;

        Ok(())
    }
}

impl bincode::Decode for ObjectMetadata {
    fn decode<D: Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            mode: bincode::Decode::decode(decoder)?,
            content_length: bincode::Decode::decode(decoder)?,
            content_md5: bincode::Decode::decode(decoder)?,
            last_modified: {
                let v: Option<(SystemTime, (i8, i8, i8))> = bincode::Decode::decode(decoder)?;

                v.map(|(system_time, (h, m, s))| {
                    OffsetDateTime::from(system_time)
                        .to_offset(UtcOffset::from_hms(h, m, s).expect("utf offset must be valid"))
                })
            },
            etag: bincode::Decode::decode(decoder)?,
        })
    }
}

impl ObjectMetadata {
    /// Create a new object metadata
    pub fn new(mode: ObjectMode) -> Self {
        Self {
            mode,

            content_length: None,
            content_md5: None,
            last_modified: None,
            etag: None,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_metadata_bincode() {
        let expected = ObjectMetadata {
            mode: ObjectMode::FILE,
            content_length: Some(123),
            content_md5: None,
            last_modified: Some(
                OffsetDateTime::from_unix_timestamp(1024 * 1024)
                    .unwrap()
                    .replace_offset(UtcOffset::from_hms(1, 2, 3).unwrap()),
            ),
            etag: Some("hello, world!".to_string()),
        };

        let bs = bincode::encode_to_vec(&expected, bincode::config::standard())
            .expect("bincode encode must succeed");

        let (actual, _) = bincode::decode_from_slice(&bs, bincode::config::standard())
            .expect("bincode decode must succeed");

        assert_eq!(expected, actual)
    }
}
