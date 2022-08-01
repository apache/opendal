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

use std::io::ErrorKind;
use std::ops::Deref;

#[cfg(feature = "rustls")]
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
#[cfg(not(feature = "rustls"))]
use hyper_tls::HttpsConnector;

use percent_encoding::utf8_percent_encode;
use percent_encoding::AsciiSet;
use percent_encoding::NON_ALPHANUMERIC;

/// HttpClient that used across opendal.
///
/// NOTE: we could change or support more underlying http backend.
#[derive(Debug, Clone)]
pub struct HttpClient(hyper::Client<HttpsConnector<hyper::client::HttpConnector>, hyper::Body>);

#[cfg(not(feature = "rustls"))]
#[inline]
pub(crate) fn https_connector() -> HttpsConnector<hyper::client::HttpConnector> {
    HttpsConnector::new()
}

#[cfg(feature = "rustls")]
#[inline]
pub(crate) fn https_connector() -> HttpsConnector<hyper::client::HttpConnector> {
    HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .build()
}

impl HttpClient {
    /// Create a new http client.
    pub fn new() -> Self {
        HttpClient(
            hyper::Client::builder()
                // Disable connection pool to address weird async runtime hang.
                //
                // ref: https://github.com/datafuselabs/opendal/issues/473
                .pool_max_idle_per_host(0)
                .build(https_connector()),
        )
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::new()
    }
}

/// Forward all function to http backend.
impl Deref for HttpClient {
    type Target = hyper::Client<HttpsConnector<hyper::client::HttpConnector>, hyper::Body>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Parse hyper error into `ErrorKind`.
///
/// It's safe to retry the following errors:
///
/// - is_canceled(): Request that was canceled.
/// - is_connect(): an error from Connect.
/// - is_timeout(): the error was caused by a timeout.
pub fn parse_error_kind(err: &hyper::Error) -> ErrorKind {
    if err.is_canceled() || err.is_connect() || err.is_timeout() {
        ErrorKind::Interrupted
    } else {
        ErrorKind::Other
    }
}

/// PATH_ENCODE_SET is the encode set for http url path.
///
/// This set follows [encodeURIComponent](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/encodeURIComponent) which will encode all non-ASCII characters except `A-Z a-z 0-9 - _ . ! ~ * ' ( )`
///
/// There is a special case for `/` in path: we will allow `/` in path as
/// required by storage services like s3.
static PATH_ENCODE_SET: AsciiSet = NON_ALPHANUMERIC
    .remove(b'/')
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

/// percent_encode_path will do percent encoding for http encode path.
///
/// Follows [encodeURIComponent](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/encodeURIComponent) which will encode all non-ASCII characters except `A-Z a-z 0-9 - _ . ! ~ * ' ( )`
///
/// There is a special case for `/` in path: we will allow `/` in path as
/// required by storage services like s3.
pub fn percent_encode_path(path: &str) -> String {
    utf8_percent_encode(path, &PATH_ENCODE_SET).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percent_encode_path() {
        let cases = vec![
            (
                "Reserved Characters",
                ";,/?:@&=+$",
                "%3B%2C/%3F%3A%40%26%3D%2B%24",
            ),
            ("Unescaped Characters", "-_.!~*'()", "-_.!~*'()"),
            ("Number Sign", "#", "%23"),
            (
                "Alphanumeric Characters + Space",
                "ABC abc 123",
                "ABC%20abc%20123",
            ),
            (
                "Unicode",
                "你好，世界！❤",
                "%E4%BD%A0%E5%A5%BD%EF%BC%8C%E4%B8%96%E7%95%8C%EF%BC%81%E2%9D%A4",
            ),
        ];

        for (name, input, expected) in cases {
            let actual = percent_encode_path(input);

            assert_eq!(actual, expected, "{name}");
        }
    }
}
