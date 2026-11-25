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

use base64::Engine;
use base64::engine::general_purpose;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::header::CACHE_CONTROL;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_ENCODING;
use http::header::CONTENT_LANGUAGE;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_RANGE;
use http::header::CONTENT_TYPE;
use http::header::ETAG;
use http::header::LAST_MODIFIED;
use http::header::LOCATION;
use md5::Digest;

use crate::EntryMode;
use crate::Error;
use crate::ErrorKind;
use crate::Metadata;
use crate::MetadataBuilder;
use crate::Result;
use crate::raw::*;

/// Parse redirect location from header map
///
/// # Note
/// The returned value maybe a relative path, like `/index.html`, `/robots.txt`, etc.
pub fn parse_location(headers: &HeaderMap) -> Result<Option<&str>> {
    parse_header_to_str(headers, LOCATION)
}

/// Parse cache control from header map.
///
/// # Note
///
/// The returned value is the raw string of `cache-control` header,
/// maybe `no-cache`, `max-age=3600`, etc.
pub fn parse_cache_control(headers: &HeaderMap) -> Result<Option<&str>> {
    parse_header_to_str(headers, CACHE_CONTROL)
}

/// Parse content length from header map.
pub fn parse_content_length(headers: &HeaderMap) -> Result<Option<u64>> {
    parse_header_to_str(headers, CONTENT_LENGTH)?
        .map(|v| {
            v.parse::<u64>().map_err(|err| {
                Error::new(ErrorKind::Unexpected, "header value is not valid integer")
                    .set_source(err)
            })
        })
        .transpose()
}

/// Parse content md5 from header map.
pub fn parse_content_md5(headers: &HeaderMap) -> Result<Option<&str>> {
    parse_header_to_str(headers, "content-md5")
}

/// Parse content type from header map.
pub fn parse_content_type(headers: &HeaderMap) -> Result<Option<&str>> {
    parse_header_to_str(headers, CONTENT_TYPE)
}

/// Parse content encoding from header map.
pub fn parse_content_encoding(headers: &HeaderMap) -> Result<Option<&str>> {
    parse_header_to_str(headers, CONTENT_ENCODING)
}

/// Parse content range from header map.
pub fn parse_content_range(headers: &HeaderMap) -> Result<Option<BytesContentRange>> {
    parse_header_to_str(headers, CONTENT_RANGE)?
        .map(|v| v.parse())
        .transpose()
}

/// Parse last modified from header map.
pub fn parse_last_modified(headers: &HeaderMap) -> Result<Option<Timestamp>> {
    parse_header_to_str(headers, LAST_MODIFIED)?
        .map(parse_http_date)
        .transpose()
}

/// Parse an HTTP-date.
///
/// RFC 9110 Section 5.6.7 requires recipients to accept the IMF-fixdate
/// format as well as the obsolete RFC 850 and asctime formats.
fn parse_http_date(s: &str) -> Result<Timestamp> {
    let err = match Timestamp::parse_rfc2822(s) {
        Ok(ts) => return Ok(ts),
        Err(err) => err,
    };

    // RFC 850, e.g. `Sunday, 06-Nov-94 08:49:37 GMT`, and asctime, e.g.
    // `Sun Nov  6 08:49:37 1994`. Both are always in UTC.
    for format in ["%A, %d-%b-%y %H:%M:%S GMT", "%a %b %e %H:%M:%S %Y"] {
        if let Ok(ts) = jiff::fmt::strtime::parse(format, s)
            .and_then(|tm| tm.to_datetime())
            .and_then(|dt| jiff::tz::Offset::UTC.to_timestamp(dt))
        {
            return Ok(ts.into());
        }
    }

    Err(err)
}

/// Parse etag from header map.
pub fn parse_etag(headers: &HeaderMap) -> Result<Option<&str>> {
    parse_header_to_str(headers, ETAG)
}

/// Parse Content-Disposition for header map
pub fn parse_content_disposition(headers: &HeaderMap) -> Result<Option<&str>> {
    parse_header_to_str(headers, CONTENT_DISPOSITION)
}

/// Parse multipart boundary from header map.
pub fn parse_multipart_boundary(headers: &HeaderMap) -> Result<Option<&str>> {
    parse_header_to_str(headers, CONTENT_TYPE).map(|v| v.and_then(|v| v.split("boundary=").nth(1)))
}

/// Parse header value to string according to name.
#[inline]
pub fn parse_header_to_str<K>(headers: &HeaderMap, name: K) -> Result<Option<&str>>
where
    HeaderName: TryFrom<K>,
{
    let name = HeaderName::try_from(name).map_err(|_| {
        Error::new(
            ErrorKind::Unexpected,
            "header name must be valid http header name but not",
        )
        .with_operation("http_util::parse_header_to_str")
    })?;

    let value = if let Some(v) = headers.get(&name) {
        v
    } else {
        return Ok(None);
    };

    Ok(Some(value.to_str().map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "header value must be valid utf-8 string but not",
        )
        .with_operation("http_util::parse_header_to_str")
        .with_context("header_name", name.as_str())
        .set_source(e)
    })?))
}

/// parse_into_metadata will parse standards http headers into Metadata.
///
/// # Notes
///
/// parse_into_metadata only handles the standard behavior of http
/// headers. If services have their own logic, they should update the parsed
/// metadata on demand.
pub fn parse_into_metadata(path: &str, headers: &HeaderMap) -> Result<Metadata> {
    let mode = if path.ends_with('/') {
        EntryMode::DIR
    } else {
        EntryMode::FILE
    };
    let content_range = parse_content_range(headers)?;
    let content_length = match content_range {
        Some(range) => range.size(),
        None => parse_content_length(headers)?,
    };
    let mut m = if mode == EntryMode::FILE {
        MetadataBuilder::file(content_length.ok_or_else(|| {
            let message = if content_range.is_some() {
                "HTTP Content-Range header does not specify total file content length"
            } else {
                "HTTP response does not contain file content length"
            };
            Error::new(ErrorKind::Unexpected, message)
        })?)
    } else {
        MetadataBuilder::dir()
    };

    if let Some(v) = parse_cache_control(headers)? {
        m.cache_control(v);
    }

    if let Some(v) = parse_content_type(headers)? {
        m.content_type(v);
    }

    if let Some(v) = parse_content_encoding(headers)? {
        m.content_encoding(v);
    }

    if let Some(v) = parse_header_to_str(headers, CONTENT_LANGUAGE)? {
        m.content_language(v);
    }

    if let Some(v) = parse_etag(headers)? {
        m.etag(v);
    }

    if let Some(v) = parse_content_md5(headers)? {
        m.content_md5(v);
    }

    if let Some(v) = parse_last_modified(headers)? {
        m.last_modified(v);
    }

    if let Some(v) = parse_content_disposition(headers)? {
        m.content_disposition(v);
    }

    Ok(m.build())
}

/// Parse prefixed headers and return a map with the prefix of each header removed.
pub fn parse_prefixed_headers(headers: &HeaderMap, prefix: &str) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            name.as_str().strip_prefix(prefix).and_then(|stripped_key| {
                value
                    .to_str()
                    .ok()
                    .map(|parsed_value| (stripped_key.to_string(), parsed_value.to_string()))
            })
        })
        .collect()
}

/// format content md5 header by given input.
pub fn format_content_md5(bs: &[u8]) -> String {
    let mut hasher = md5::Md5::new();
    hasher.update(bs);

    general_purpose::STANDARD.encode(hasher.finalize())
}

/// format content md5 header by given iter of bytes.
pub fn format_content_md5_iter<I>(bs: I) -> String
where
    I: IntoIterator,
    I::Item: AsRef<[u8]>,
{
    let mut hasher = md5::Md5::new();

    for b in bs {
        hasher.update(b.as_ref());
    }

    general_purpose::STANDARD.encode(hasher.finalize())
}

/// format authorization header by basic auth.
///
/// # Errors
///
/// If input username is empty, function will return an unexpected error.
pub fn format_authorization_by_basic(username: &str, password: &str) -> Result<String> {
    if username.is_empty() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "can't build authorization header with empty username",
        ));
    }

    let value = general_purpose::STANDARD.encode(format!("{username}:{password}"));

    Ok(format!("Basic {value}"))
}

/// format authorization header by bearer token.
///
/// # Errors
///
/// If input token is empty, function will return an unexpected error.
pub fn format_authorization_by_bearer(token: &str) -> Result<String> {
    if token.is_empty() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "can't build authorization header with empty token",
        ));
    }

    Ok(format!("Bearer {token}"))
}

/// Build header value from given string.
pub fn build_header_value(v: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(v).map_err(|e| {
        Error::new(
            ErrorKind::ConfigInvalid,
            "header value contains invalid characters",
        )
        .with_operation("http_util::build_header_value")
        .set_source(e)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_last_modified_accepts_all_http_date_formats() {
        for value in [
            "Sun, 06 Nov 1994 08:49:37 GMT",
            "Sunday, 06-Nov-94 08:49:37 GMT",
            "Sun Nov  6 08:49:37 1994",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(LAST_MODIFIED, HeaderValue::from_static(value));
            let ts = parse_last_modified(&headers)
                .unwrap_or_else(|err| panic!("{value}: {err}"))
                .expect("last modified must be present");
            assert_eq!(ts.to_string(), "1994-11-06T08:49:37Z", "{value}");
        }

        let mut headers = HeaderMap::new();
        headers.insert(LAST_MODIFIED, HeaderValue::from_static("not a date"));
        assert!(parse_last_modified(&headers).is_err());
    }

    /// Test cases is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    #[test]
    fn test_format_content_md5() {
        let cases = vec![(
            r#"<Delete>
<Object>
 <Key>sample1.txt</Key>
 </Object>
 <Object>
   <Key>sample2.txt</Key>
 </Object>
 </Delete>"#,
            "WOctCY1SS662e7ziElh4cw==",
        )];

        for (input, expected) in cases {
            let actual = format_content_md5(input.as_bytes());

            assert_eq!(actual, expected)
        }
    }

    /// Test cases is borrowed from
    ///
    /// - RFC2617: https://datatracker.ietf.org/doc/html/rfc2617#section-2
    /// - MDN: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization
    #[test]
    fn test_format_authorization_by_basic() {
        let cases = vec![
            ("aladdin", "opensesame", "Basic YWxhZGRpbjpvcGVuc2VzYW1l"),
            ("aladdin", "", "Basic YWxhZGRpbjo="),
            (
                "Aladdin",
                "open sesame",
                "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==",
            ),
            ("Aladdin", "", "Basic QWxhZGRpbjo="),
        ];

        for (username, password, expected) in cases {
            let actual =
                format_authorization_by_basic(username, password).expect("format must success");

            assert_eq!(actual, expected)
        }
    }

    /// Test cases is borrowed from
    ///
    /// - RFC6750: https://datatracker.ietf.org/doc/html/rfc6750
    #[test]
    fn test_format_authorization_by_bearer() {
        let cases = vec![("mF_9.B5f-4.1JqM", "Bearer mF_9.B5f-4.1JqM")];

        for (token, expected) in cases {
            let actual = format_authorization_by_bearer(token).expect("format must success");

            assert_eq!(actual, expected)
        }
    }

    #[test]
    fn test_parse_multipart_boundary() {
        let cases = [
            (
                "multipart/mixed; boundary=gc0p4Jq0M2Yt08jU534c0p",
                Some("gc0p4Jq0M2Yt08jU534c0p"),
            ),
            ("multipart/mixed", None),
            // GCS returns unquoted boundaries that contain `=`.
            (
                "multipart/mixed; boundary=batch_pK7JBAk73-E=_AA5eFwv4m2Q=",
                Some("batch_pK7JBAk73-E=_AA5eFwv4m2Q="),
            ),
        ];

        for (input, expected) in cases {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, HeaderValue::from_str(input).unwrap());

            let actual = parse_multipart_boundary(&headers).expect("parse must success");

            assert_eq!(actual, expected)
        }
    }

    #[test]
    fn test_parse_multipart_boundary_missing_content_type() {
        let headers = HeaderMap::new();

        let actual = parse_multipart_boundary(&headers).expect("parse must success");

        assert_eq!(actual, None)
    }

    #[test]
    fn test_parse_multipart_boundary_non_utf8_content_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_bytes(b"multipart/mixed; boundary=\xff").unwrap(),
        );

        parse_multipart_boundary(&headers).expect_err("non-utf8 header value must fail");
    }

    #[test]
    fn test_parse_content_length_missing() {
        let headers = HeaderMap::new();

        assert_eq!(parse_content_length(&headers).unwrap(), None);
    }

    #[test]
    fn test_parse_content_length_accepts_identical_duplicates() {
        let mut headers = HeaderMap::new();
        headers.append(CONTENT_LENGTH, HeaderValue::from_static("10"));
        headers.append(CONTENT_LENGTH, HeaderValue::from_static("10"));

        assert_eq!(parse_content_length(&headers).unwrap(), Some(10));
    }

    #[test]
    fn test_parse_content_length_rejects_invalid_values() {
        for value in ["-1", " 10", "10 ", "", "10, 10", "10abc"] {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_LENGTH, HeaderValue::from_str(value).unwrap());

            let err = parse_content_length(&headers).expect_err("invalid content length must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected, "value: {value:?}");
        }
    }

    #[test]
    fn test_parse_into_metadata_content_range_unknown_size_does_not_use_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 0-3/*"));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("4"));

        let err = parse_into_metadata("file.txt", &headers)
            .expect_err("unknown total size in Content-Range must not fall back to Content-Length");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("Content-Range"));
    }

    #[test]
    fn test_parse_into_metadata_content_range_known_size() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_RANGE, HeaderValue::from_static("bytes 0-3/10"));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("4"));

        let meta = parse_into_metadata("file.txt", &headers).expect("parse must succeed");
        assert_eq!(meta.content_length(), 10);
    }

    #[test]
    fn test_parse_into_metadata_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("10"));

        let meta = parse_into_metadata("file.txt", &headers).expect("parse must succeed");
        assert_eq!(meta.content_length(), 10);
    }
}
