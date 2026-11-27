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
use std::time::Duration;

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
use http::header::DATE;
use http::header::ETAG;
use http::header::LAST_MODIFIED;
use http::header::LOCATION;
use http::header::RETRY_AFTER;
use jiff::ToSpan;
use jiff::tz::Offset;
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

/// Parse a positive `Retry-After` delay, in seconds or as an HTTP date.
///
/// For a date, use the response's `Date` header as the reference when valid,
/// and use `now` otherwise. Missing, ambiguous, invalid, and elapsed hints
/// return `None`.
pub fn parse_retry_after(headers: &HeaderMap, now: Timestamp) -> Option<Duration> {
    let single_value = |name| {
        let mut values = headers.get_all(name).iter();
        match (values.next(), values.next()) {
            (Some(value), None) => value.to_str().ok().map(|s| s.trim_matches([' ', '\t'])),
            _ => None,
        }
    };
    let value = single_value(RETRY_AFTER)?;
    let delay = if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        Duration::from_secs(value.parse().ok()?)
    } else {
        let now = now.into_inner();
        let (retry_at, retry_leap_second) = parse_retry_after_date(value, now)?;
        let (response_date, response_leap_second) = single_value(DATE)
            .and_then(|value| parse_retry_after_date(value, now))
            .unwrap_or((now, false));
        let mut delay = retry_at.duration_since(response_date);
        // Jiff represents an inserted leap second as 23:59:59.
        if retry_leap_second && !(response_leap_second && retry_at == response_date) {
            delay += jiff::SignedDuration::from_secs(1);
        }
        Duration::try_from(delay).ok()?
    };
    (!delay.is_zero()).then_some(delay)
}

fn parse_retry_after_date(value: &str, now: jiff::Timestamp) -> Option<(jiff::Timestamp, bool)> {
    const RFC850: &str = "%A, %d-%b-%y %T GMT";
    let format = match value.find(',') {
        Some(3) => "%a, %d %b %Y %T GMT",
        Some(_) => RFC850,
        None if value.as_bytes().get(8) == Some(&b' ') => "%a %b %e %T %Y",
        None => "%a %b %d %T %Y",
    };
    let mut fields = jiff::fmt::strtime::parse(format, value).ok()?;
    let date = if format == RFC850 {
        // RFC 9110 resolves RFC 850 years in a rolling 100-year window.
        let now = Offset::UTC.to_datetime(now);
        let start = now.checked_sub(50.years()).ok()?;
        let end = now.checked_add(50.years()).ok()?;
        let year = end.year() / 100 * 100 + fields.year()? % 100;
        [year, year - 100].into_iter().find_map(|year| {
            fields.set_year(Some(year)).ok()?;
            let date = fields.to_datetime().ok()?;
            (start < date && date <= end).then_some(date)
        })?
    } else {
        fields.to_datetime().ok()?
    };
    let leap_second = value.contains("23:59:60") && date.day() == date.days_in_month();
    let mut canonical = date.strftime(format).to_string();
    if leap_second {
        canonical = canonical.replace("23:59:59", "23:59:60");
    }
    // Strptime also accepts non-HTTP syntax, such as mismatched weekdays.
    if canonical != value {
        return None;
    }
    Some((Offset::UTC.to_timestamp(date).ok()?, leap_second))
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
    parse_header_to_str(headers, CONTENT_TYPE).map(|v| v.and_then(boundary_parameter))
}

/// Take the `boundary` parameter and stop at the next parameter.
///
/// A quoted value may contain `=` or `;`. An unquoted value ends at `;`, so a
/// GCS boundary that itself contains `=` is kept intact.
fn boundary_parameter(content_type: &str) -> Option<&str> {
    let rest = content_type.split("boundary=").nth(1)?.trim_start();
    if let Some(quoted) = rest.strip_prefix('"') {
        let end = quoted.find('"')?;
        let token = &quoted[..end];
        (!token.is_empty()).then_some(token)
    } else {
        let token = rest.split(';').next()?.trim();
        (!token.is_empty()).then_some(token)
    }
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

    #[test]
    fn test_parse_retry_after_seconds() {
        let now = Timestamp::default();
        let mut headers = HeaderMap::new();
        assert_eq!(parse_retry_after(&headers, now), None);
        headers.insert(RETRY_AFTER, HeaderValue::from_static(" \t033 "));
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(33))
        );
        headers.append(RETRY_AFTER, HeaderValue::from_static("33"));
        assert_eq!(parse_retry_after(&headers, now), None);

        for value in ["", "0", "+1", "-1", "1.5", "1, 2", "18446744073709551616"] {
            headers.insert(RETRY_AFTER, HeaderValue::from_static(value));
            assert_eq!(parse_retry_after(&headers, now), None, "{value:?}");
        }
        headers.insert(RETRY_AFTER, HeaderValue::from_bytes(b"\xff").unwrap());
        assert_eq!(parse_retry_after(&headers, now), None);
    }

    #[test]
    fn test_parse_retry_after_dates() {
        let now = "2026-11-26T01:00:00Z".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            DATE,
            HeaderValue::from_static("Sun, 06 Nov 1994 08:49:30 GMT"),
        );
        for value in [
            "Sun, 06 Nov 1994 08:49:37 GMT",
            "Sunday, 06-Nov-94 08:49:37 GMT",
            "Sun Nov  6 08:49:37 1994",
            "Sun Nov 06 08:49:37 1994",
        ] {
            headers.insert(RETRY_AFTER, HeaderValue::from_static(value));
            assert_eq!(
                parse_retry_after(&headers, now),
                Some(Duration::from_secs(7)),
                "{value}"
            );
        }
        for value in [
            "bad date",
            "Sun, 06 Nov 1994 08:49:30 GMT",
            "Sun, 06 Nov 1994 08:49:29 GMT",
            "Mon, 06 Nov 1994 08:49:37 GMT",
            "Sun, 06 Nov 1994 08:49:37 +0000",
        ] {
            headers.insert(RETRY_AFTER, HeaderValue::from_static(value));
            assert_eq!(parse_retry_after(&headers, now), None, "{value}");
        }
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Thu, 26 Nov 2026 01:00:05 GMT"),
        );
        headers.append(
            DATE,
            HeaderValue::from_static("Sun, 06 Nov 1994 08:49:30 GMT"),
        );
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(5))
        );
        headers.insert(DATE, HeaderValue::from_static("bad date"));
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(5))
        );
        headers.remove(DATE);
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(5))
        );
    }

    #[test]
    fn test_parse_retry_after_uses_rolling_year_and_preserves_leap_second() {
        let now = "2026-11-21T00:00:00Z".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            DATE,
            HeaderValue::from_static("Fri, 20 Nov 2076 23:59:59 GMT"),
        );
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Saturday, 21-Nov-76 00:00:00 GMT"),
        );
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(1))
        );
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Sunday, 21-Nov-76 00:00:01 GMT"),
        );
        assert_eq!(parse_retry_after(&headers, now), None);

        headers.insert(
            DATE,
            HeaderValue::from_static("Sat, 31 Dec 2016 23:59:59 GMT"),
        );
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Sat, 31 Dec 2016 23:59:60 GMT"),
        );
        assert_eq!(
            parse_retry_after(&headers, now),
            Some(Duration::from_secs(1))
        );
        headers.insert(DATE, headers[RETRY_AFTER].clone());
        assert_eq!(parse_retry_after(&headers, now), None);
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
            (
                "multipart/mixed; boundary=gc0p4Jq0M2Yt08jU534c0p; charset=utf-8",
                Some("gc0p4Jq0M2Yt08jU534c0p"),
            ),
            (
                "multipart/mixed; boundary=\"gc0p4Jq0M2Yt08jU534c0p\"",
                Some("gc0p4Jq0M2Yt08jU534c0p"),
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
