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

use crate::*;
use percent_encoding::percent_decode_str;
use percent_encoding::utf8_percent_encode;
use percent_encoding::AsciiSet;
use percent_encoding::NON_ALPHANUMERIC;

/// Parse http uri invalid error in to opendal::Error.
pub fn new_http_uri_invalid_error(err: http::uri::InvalidUri) -> Error {
    Error::new(ErrorKind::Unexpected, "parse http uri").set_source(err)
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

/// percent_decode_path will do percent decoding for http decode path.
///
/// If the input is not percent encoded or not valid utf8, return the input.
pub fn percent_decode_path(path: &str) -> String {
    match percent_decode_str(path).decode_utf8() {
        Ok(v) => v.to_string(),
        Err(_) => path.to_string(),
    }
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

    #[test]
    fn test_percent_decode_path() {
        let cases = vec![
            (
                "Reserved Characters",
                "%3B%2C/%3F%3A%40%26%3D%2B%24",
                ";,/?:@&=+$",
            ),
            ("Unescaped Characters", "-_.!~*'()", "-_.!~*'()"),
            ("Number Sign", "%23", "#"),
            (
                "Alphanumeric Characters + Space",
                "ABC%20abc%20123",
                "ABC abc 123",
            ),
            (
                "Unicode Characters",
                "%E4%BD%A0%E5%A5%BD%EF%BC%8C%E4%B8%96%E7%95%8C%EF%BC%81%E2%9D%A4",
                "你好，世界！❤",
            ),
            (
                "Double Encoded Characters",
                "Double%2520Encoded",
                "Double%20Encoded",
            ),
            (
                "Not Percent Encoded Characters",
                "/not percent encoded/path;,/?:@&=+$-",
                "/not percent encoded/path;,/?:@&=+$-",
            ),
        ];

        for (name, input, expected) in cases {
            let actual = percent_decode_path(input);

            assert_eq!(actual, expected, "{name}");
        }
    }
}
