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

use std::time::Duration;

use http::HeaderMap;
use http::Uri;
use http::header::{DATE, RETRY_AFTER};
use http::response::Parts;
use jiff::ToSpan;
use jiff::tz::Offset;

use crate::Error;
use crate::ErrorKind;
use crate::raw::Timestamp;

/// Create a new error happened during building request.
pub fn new_request_build_error(err: http::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "building http request")
        .with_operation("http::Request::build")
        .set_source(err)
}

/// Create a new error happened during signing request.
pub fn new_request_credential_error(err: anyhow::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "loading credential to sign http request",
    )
    .set_temporary()
    .with_operation("reqsign::LoadCredential")
    .set_source(err)
}

/// Wrap a signing failure, preserving reqsign's retryability when available.
pub fn new_request_sign_error(err: anyhow::Error) -> Error {
    let error = Error::new(ErrorKind::Unexpected, "signing http request");
    #[cfg(feature = "reqsign")]
    let error = error.with_temporary(
        err.downcast_ref::<reqsign_core::Error>()
            .is_some_and(reqsign_core::Error::is_retryable),
    );
    error.with_operation("reqsign::Sign").set_source(err)
}

/// Add response context to error.
///
/// This helper function will:
///
/// - remove sensitive or useless headers from parts.
/// - fetch uri if parts extensions contains `Uri`.
/// - attach the service's requested retry delay, if present.
pub fn with_error_response_context(mut err: Error, mut parts: Parts) -> Error {
    if let Some(uri) = parts.extensions.get::<Uri>() {
        err = err.with_context("uri", uri.to_string());
    }

    if let Some(delay) = parse_retry_after(&parts.headers) {
        err = err.with_retry_after(delay);
    }

    // The following headers may contains sensitive information.
    parts.headers.remove("Set-Cookie");
    parts.headers.remove("WWW-Authenticate");
    parts.headers.remove("Proxy-Authenticate");

    if parts.headers.contains_key(http::header::LOCATION) {
        let location = crate::HttpUri::from_response_location(&mut parts)
            .map(|uri| uri.redacted_uri())
            .unwrap_or("<invalid URL>")
            .parse()
            .unwrap_or_else(|_| http::HeaderValue::from_static("<invalid URL>"));
        parts.headers.insert(http::header::LOCATION, location);
    }

    err = err.with_context("response", format!("{parts:?}"));

    err
}

fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let single_value = |name| {
        let mut values = headers.get_all(name).iter();
        match (values.next(), values.next()) {
            (Some(value), None) => value.to_str().ok().map(str::trim),
            _ => None,
        }
    };
    let value = single_value(RETRY_AFTER)?;
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        // An overflowing delay still expresses a minimum wait.
        return Some(
            value
                .parse()
                .map(Duration::from_secs)
                .unwrap_or(Duration::MAX),
        );
    }

    let now = Timestamp::now().into_inner();
    let (retry_at, retry_leap) = parse_http_date(value, now)?;
    let (date, date_leap) = single_value(DATE)
        .and_then(|value| parse_http_date(value, now))
        .unwrap_or((now, false));
    let mut delay = retry_at.duration_since(date);
    // Jiff maps the inserted second to 23:59:59. Count it if the deadline
    // is that extra second, unless both fields denote the same instant.
    if retry_leap && !(date_leap && retry_at == date) {
        delay += jiff::SignedDuration::from_secs(1);
    }
    Some(delay.try_into().unwrap_or_default())
}

fn parse_http_date(value: &str, now: jiff::Timestamp) -> Option<(jiff::Timestamp, bool)> {
    const RFC850: &str = "%A, %d-%b-%y %T GMT";
    let format = match value.find(',') {
        Some(3) => "%a, %d %b %Y %T GMT",
        Some(_) => RFC850,
        // The asctime format accepts both space-padded and two-digit days.
        None if value.as_bytes().get(8) == Some(&b' ') => "%a %b %e %T %Y",
        None => "%a %b %d %T %Y",
    };
    let mut fields = jiff::fmt::strtime::parse(format, value).ok()?;
    let date = if format == RFC850 {
        // RFC 9110 §5.6.7 resolves two-digit years in a rolling 100-year window.
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
    // Jiff maps a leap second to :59. Keep it distinct when comparing HTTP dates.
    let leap_second = value.contains("23:59:60") && date.day() == date.days_in_month();
    let mut canonical = date.strftime(format).to_string();
    if leap_second {
        canonical = canonical.replace("23:59:59", "23:59:60");
    }
    // strptime accepts non-canonical spacing, case and field widths.
    if date.year() < 1900 || canonical != value {
        return None;
    }
    Some((Offset::UTC.to_timestamp(date).ok()?, leap_second))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response_error(retry_after: &[&str], date: &[&str]) -> Error {
        let mut response = http::Response::builder();
        for &value in retry_after {
            response = response.header(RETRY_AFTER, value);
        }
        for &value in date {
            response = response.header(DATE, value);
        }
        let (parts, _) = response.body(()).unwrap().into_parts();
        with_error_response_context(Error::new(ErrorKind::RateLimited, ""), parts)
    }

    #[test]
    fn retry_after_seconds() {
        for (value, secs) in [("120", 120), ("0", 0), ("18446744073709551615", u64::MAX)] {
            let err = response_error(&[value], &[]);
            assert_eq!(err.retry_after(), Some(Duration::from_secs(secs)));
            assert!(err.is_permanent());
        }
        assert_eq!(
            response_error(&["18446744073709551616"], &[]).retry_after(),
            Some(Duration::MAX)
        );
    }

    #[test]
    fn retry_after_ignores_missing_or_malformed_values() {
        assert_eq!(response_error(&[], &[]).retry_after(), None);
        assert_eq!(response_error(&["2", "400"], &[]).retry_after(), None);
        for value in [
            "",
            "-1",
            "+1",
            "0.5",
            "2, 400",
            "bad date",
            "Sun, 01 Jan 1899 00:00:00 GMT",
            "Mon, 06 Nov 1994 08:49:37 GMT",
            "sun, 06 Nov 1994 08:49:37 GMT",
        ] {
            assert_eq!(response_error(&[value], &[]).retry_after(), None);
        }
    }

    #[test]
    fn retry_after_http_dates_use_the_response_date() {
        let date = &["Sun, 06 Nov 1994 08:49:30 GMT"];
        for value in [
            "Sun, 06 Nov 1994 08:49:37 GMT",
            "Sun Nov  6 08:49:37 1994",
            "Sun Nov 06 08:49:37 1994",
        ] {
            assert_eq!(
                response_error(&[value], date).retry_after(),
                Some(Duration::from_secs(7))
            );
        }
        assert_eq!(
            response_error(&["Sun, 06 Nov 1994 08:49:29 GMT"], date).retry_after(),
            Some(Duration::ZERO)
        );
        let before_epoch = &["Wed, 31 Dec 1969 23:59:59 GMT"];
        let epoch = &["Thu, 01 Jan 1970 00:00:00 GMT"];
        assert_eq!(
            response_error(epoch, before_epoch).retry_after(),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            response_error(before_epoch, epoch).retry_after(),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn retry_after_rfc850_dates_resolve_two_digit_years() {
        let now = "2026-11-21T00:00:00Z".parse().unwrap();
        for (value, expected) in [
            ("Saturday, 21-Nov-76 00:00:00 GMT", "2076-11-21T00:00:00Z"),
            ("Sunday, 21-Nov-76 00:00:01 GMT", "1976-11-21T00:00:01Z"),
        ] {
            assert_eq!(
                parse_http_date(value, now),
                Some((expected.parse().unwrap(), false))
            );
        }
    }

    #[test]
    fn retry_after_uses_the_local_clock_without_a_valid_response_date() {
        let now = || std::time::SystemTime::from(Timestamp::now().into_inner());
        let value = (Timestamp::now() + Duration::from_secs(3600)).format_http_date();
        let retry_at =
            std::time::SystemTime::from(Timestamp::parse_rfc2822(&value).unwrap().into_inner());
        let duplicate = [
            "Sun, 06 Nov 1994 08:49:30 GMT",
            "Sun, 06 Nov 1994 08:49:31 GMT",
        ];
        for date in [&[][..], &["bad date"], &duplicate] {
            let before = now();
            let delay = response_error(&[&value], date).retry_after().unwrap();
            let after = now();
            assert!(delay >= retry_at.duration_since(after).unwrap());
            assert!(delay <= retry_at.duration_since(before).unwrap());
        }
    }

    #[test]
    fn retry_after_explicit_leap_seconds() {
        let before = "Sat, 31 Dec 2016 23:59:59 GMT";
        let leap = "Sat, 31 Dec 2016 23:59:60 GMT";
        let after = "Sun, 01 Jan 2017 00:00:00 GMT";
        for (target, date, seconds) in [(leap, before, 1), (after, leap, 1), (leap, leap, 0)] {
            assert_eq!(
                response_error(&[target], &[date]).retry_after(),
                Some(Duration::from_secs(seconds))
            );
        }
        let now = "2017-01-01T00:00:00Z".parse().unwrap();
        let expected = Some(("2016-12-31T23:59:59Z".parse().unwrap(), true));
        assert_eq!(
            parse_http_date("Saturday, 31-Dec-16 23:59:60 GMT", now),
            expected
        );
        assert_eq!(parse_http_date("Sat Dec 31 23:59:60 2016", now), expected);
        for invalid in [
            "Sat, 31 Dec 2016 22:59:60 GMT",
            "Fri, 30 Dec 2016 23:59:60 GMT",
        ] {
            assert_eq!(parse_http_date(invalid, now), None);
        }
    }
}
