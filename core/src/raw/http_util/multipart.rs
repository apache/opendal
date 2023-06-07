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

use std::io::BufRead;
use std::mem;
use std::str::FromStr;

use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::uri::PathAndQuery;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::Uri;
use http::Version;

use super::new_request_build_error;
use super::AsyncBody;
use crate::*;

/// Multipart is a builder for multipart/form-data.
#[derive(Debug)]
pub struct Multipart<T: Part> {
    boundary: String,
    parts: Vec<T>,
}

impl<T: Part> Default for Multipart<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Part> Multipart<T> {
    /// Create a new multipart with random boundary.
    pub fn new() -> Self {
        Multipart {
            boundary: format!("opendal-{}", uuid::Uuid::new_v4()),
            parts: Vec::default(),
        }
    }

    /// Set the boundary with given string.
    #[cfg(test)]
    fn with_boundary(mut self, boundary: &str) -> Self {
        self.boundary = boundary.to_string();
        self
    }

    /// Insert a part into multipart.
    pub fn part(mut self, part: T) -> Self {
        self.parts.push(part);
        self
    }

    pub(crate) fn build(&self) -> Bytes {
        let mut bs = BytesMut::new();

        // Write headers.
        for v in self.parts.iter() {
            // Write the first boundary
            bs.extend_from_slice(b"--");
            bs.extend_from_slice(self.boundary.as_bytes());
            bs.extend_from_slice(b"\r\n");

            bs.extend_from_slice(v.format().as_ref());
        }

        // Write the last boundary
        bs.extend_from_slice(b"--");
        bs.extend_from_slice(self.boundary.as_bytes());
        bs.extend_from_slice(b"--");
        bs.extend_from_slice(b"\r\n");

        bs.freeze()
    }

    /// Consume the input and generate a request with multipart body.
    ///
    /// This founction will make sure content_type and content_length set correctly.
    pub fn apply(self, mut builder: http::request::Builder) -> Result<Request<AsyncBody>> {
        let bs = self.build();

        // Insert content type with correct boundary.
        builder = builder.header(
            CONTENT_TYPE,
            format!("multipart/{}; boundary={}", T::TYPE, self.boundary).as_str(),
        );
        // Insert content length with calculated size.
        builder = builder.header(CONTENT_LENGTH, bs.len());

        builder
            .body(AsyncBody::Bytes(bs))
            .map_err(new_request_build_error)
    }
}

/// Part is a trait for multipart part.
pub trait Part: Sized {
    /// TYPE is the type of multipart.
    ///
    /// Current available types are: `form-data` and `mixed`
    const TYPE: &'static str;

    /// format will generates the bytes.
    fn format(&self) -> Bytes;

    /// parse will parse the bytes into a part.
    fn parse(bs: Bytes) -> Result<Self>;
}

/// FormDataPart is a builder for multipart/form-data part.
#[derive(Debug)]
pub struct FormDataPart {
    headers: HeaderMap,
    content: Bytes,
}

impl FormDataPart {
    /// Create a new part builder
    ///
    /// # Panics
    ///
    /// Input name must be percent encoded.
    pub fn new(name: &str) -> Self {
        // Insert content disposition header for part.
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_DISPOSITION,
            format!("form-data; name=\"{}\"", name).parse().unwrap(),
        );

        Self {
            headers,
            content: Bytes::new(),
        }
    }

    /// Insert a header into part.
    pub fn header(mut self, key: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Set the content for this part.
    pub fn content(mut self, content: impl Into<Bytes>) -> Self {
        self.content = content.into();
        self
    }
}

impl Part for FormDataPart {
    const TYPE: &'static str = "form-data";

    fn format(&self) -> Bytes {
        let mut bs = BytesMut::new();

        // Write headers.
        for (k, v) in self.headers.iter() {
            bs.extend_from_slice(k.as_str().as_bytes());
            bs.extend_from_slice(b": ");
            bs.extend_from_slice(v.as_bytes());
            bs.extend_from_slice(b"\r\n");
        }

        // Write content.
        bs.extend_from_slice(b"\r\n");
        bs.extend_from_slice(&self.content);
        bs.extend_from_slice(b"\r\n");

        bs.freeze()
    }

    fn parse(_: Bytes) -> Result<Self> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "parse of form-data is not supported",
        ))
    }
}

/// MixedPart is a builder for multipart/mixed part.
#[derive(Debug)]
pub struct MixedPart {
    part_headers: HeaderMap,

    method: Method,
    uri: Uri,
    version: Version,
    headers: HeaderMap,
    content: Bytes,
}

impl MixedPart {
    /// Create a new mixed part with gien uri.
    pub fn new(uri: &str) -> Self {
        let mut part_headers = HeaderMap::new();
        part_headers.insert(CONTENT_TYPE, "application/http".parse().unwrap());
        part_headers.insert("content-transfer-encoding", "binary".parse().unwrap());

        let uri = Uri::from_str(uri).expect("the uri used to build a mixed part must be valid");

        Self {
            part_headers,
            method: Method::GET,
            uri,
            version: Version::HTTP_11,
            headers: HeaderMap::new(),
            content: Bytes::new(),
        }
    }

    /// Build a mixed part from a request.
    ///
    /// # Notes
    ///
    /// Mixed parts only takes the path from the request uri.
    pub fn from_request(req: Request<AsyncBody>) -> Self {
        let mut part_headers = HeaderMap::new();
        part_headers.insert(CONTENT_TYPE, "application/http".parse().unwrap());
        part_headers.insert("content-transfer-encoding", "binary".parse().unwrap());

        let (parts, body) = req.into_parts();

        let content = match body {
            AsyncBody::Empty => Bytes::new(),
            AsyncBody::Bytes(bs) => bs,
        };

        Self {
            part_headers,
            method: parts.method,
            uri: Uri::from_str(
                parts
                    .uri
                    .path_and_query()
                    .unwrap_or(&PathAndQuery::from_static("/"))
                    .as_str(),
            )
            .expect("the uri used to build a mixed part must be valid"),
            version: parts.version,
            headers: parts.headers,
            content,
        }
    }

    /// Insert a part header into part.
    pub fn part_header(mut self, key: HeaderName, value: HeaderValue) -> Self {
        self.part_headers.insert(key, value);
        self
    }

    /// Set the method for request in this part.
    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    /// Set the version for request in this part.
    pub fn version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    /// Insert a header into part.
    pub fn header(mut self, key: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Set the content for this part.
    pub fn content(mut self, content: impl Into<Bytes>) -> Self {
        self.content = content.into();
        self
    }
}

impl Part for MixedPart {
    const TYPE: &'static str = "mixed";

    fn format(&self) -> Bytes {
        let mut bs = BytesMut::new();

        // Write parts headers.
        for (k, v) in self.part_headers.iter() {
            // Trick!
            //
            // Azblob could not recognize header names like `content-type`
            // and requires to use `Content-Type`. So we hardcode the part
            // headers name here.
            match k.as_str() {
                "content-type" => {
                    bs.extend_from_slice("Content-Type".as_bytes());
                }
                "content-id" => {
                    bs.extend_from_slice("Content-ID".as_bytes());
                }
                "content-transfer-encoding" => {
                    bs.extend_from_slice("Content-Transfer-Encoding".as_bytes());
                }
                _ => {
                    bs.extend_from_slice(k.as_str().as_bytes());
                }
            }
            bs.extend_from_slice(b": ");
            bs.extend_from_slice(v.as_bytes());
            bs.extend_from_slice(b"\r\n");
        }

        // Write request line: `DELETE /container0/blob0 HTTP/1.1`
        bs.extend_from_slice(b"\r\n");
        bs.extend_from_slice(self.method.as_str().as_bytes());
        bs.extend_from_slice(b" ");
        bs.extend_from_slice(self.uri.path().as_bytes());
        bs.extend_from_slice(b" ");
        bs.extend_from_slice(format!("{:?}", self.version).as_bytes());
        bs.extend_from_slice(b"\r\n");

        // Write request headers.
        for (k, v) in self.headers.iter() {
            bs.extend_from_slice(k.as_str().as_bytes());
            bs.extend_from_slice(b": ");
            bs.extend_from_slice(v.as_bytes());
            bs.extend_from_slice(b"\r\n");
        }

        // Write content.
        bs.extend_from_slice(b"\r\n");
        if !self.content.is_empty() {
            bs.extend_from_slice(&self.content);
            bs.extend_from_slice(b"\r\n");
        }

        bs.freeze()
    }

    fn parse(bs: Bytes) -> Result<Self> {
        let mut r = bs.reader();
        let mut buf = String::new();

        let mut part_headers = HeaderMap::new();
        loop {
            let n = r.read_line(&mut buf)?;
            // Read an unexpected EOF
            if n == 0 {
                return Err(
                    Error::new(ErrorKind::Unexpected, "unexpected end of multipart")
                        .with_operation("Multipart::parse"),
                );
            }
            // Read a `\r\n`, which means the end of headers.
            if n == 1 && buf == "\r" {
                break;
            }
            // Read a `\n`, keep going.
            if n == 1 {
                continue;
            }

            let (header_name, header_value) = buf.split_once(": ").ok_or({
                Error::new(ErrorKind::Unexpected, "invalid header format")
                    .with_operation("Multipart::parse")
            })?;
            part_headers.insert(
                HeaderName::from_str(header_name).map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "invalid header name").set_source(err)
                })?,
                HeaderValue::from_str(header_value).map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "invalid header value").set_source(err)
                })?,
            );

            buf.clear();
        }
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use http::header::CONTENT_TYPE;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_multipart_formdata_basic() {
        let multipart = Multipart::new()
            .with_boundary("lalala")
            .part(FormDataPart::new("foo").content(Bytes::from("bar")))
            .part(FormDataPart::new("hello").content(Bytes::from("world")));

        let body = multipart.build();

        let expected = "--lalala\r\n\
             content-disposition: form-data; name=\"foo\"\r\n\
             \r\n\
             bar\r\n\
             --lalala\r\n\
             content-disposition: form-data; name=\"hello\"\r\n\
             \r\n\
             world\r\n\
             --lalala--\r\n";

        assert_eq!(expected, String::from_utf8(body.to_vec()).unwrap());
    }

    /// This test is inspired by <https://docs.aws.amazon.com/AmazonS3/latest/userguide/HTTPPOSTExamples.html>
    #[test]
    fn test_multipart_formdata_s3_form_upload() {
        let multipart = Multipart::new()
            .with_boundary("9431149156168")
            .part(FormDataPart::new("key").content("user/eric/MyPicture.jpg"))
            .part(FormDataPart::new("acl").content("public-read"))
            .part(FormDataPart::new("success_action_redirect").content(
                "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/successful_upload.html",
            ))
            .part(FormDataPart::new("content-type").content("image/jpeg"))
            .part(FormDataPart::new("x-amz-meta-uuid").content("14365123651274"))
            .part(FormDataPart::new("x-amz-meta-tag").content("Some,Tag,For,Picture"))
            .part(FormDataPart::new("AWSAccessKeyId").content("AKIAIOSFODNN7EXAMPLE"))
            .part(FormDataPart::new("Policy").content("eyAiZXhwaXJhdGlvbiI6ICIyMDA3LTEyLTAxVDEyOjAwOjAwLjAwMFoiLAogICJjb25kaXRpb25zIjogWwogICAgeyJidWNrZXQiOiAiam9obnNtaXRoIn0sCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci9lcmljLyJdLAogICAgeyJhY2wiOiAicHVibGljLXJlYWQifSwKICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL2pvaG5zbWl0aC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwKICAgIFsic3RhcnRzLXdpdGgiLCAiJENvbnRlbnQtVHlwZSIsICJpbWFnZS8iXSwKICAgIHsieC1hbXotbWV0YS11dWlkIjogIjE0MzY1MTIzNjUxMjc0In0sCiAgICBbInN0YXJ0cy13aXRoIiwgIiR4LWFtei1tZXRhLXRhZyIsICIiXQogIF0KfQo="))
            .part(FormDataPart::new("Signature").content("0RavWzkygo6QX9caELEqKi9kDbU="))
            .part(FormDataPart::new("file").header(CONTENT_TYPE, "image/jpeg".parse().unwrap()).content("...file content...")).part(FormDataPart::new("submit").content("Upload to Amazon S3"));

        let body = multipart.build();

        let expected = r#"--9431149156168
content-disposition: form-data; name="key"

user/eric/MyPicture.jpg
--9431149156168
content-disposition: form-data; name="acl"

public-read
--9431149156168
content-disposition: form-data; name="success_action_redirect"

https://awsexamplebucket1.s3.us-west-1.amazonaws.com/successful_upload.html
--9431149156168
content-disposition: form-data; name="content-type"

image/jpeg
--9431149156168
content-disposition: form-data; name="x-amz-meta-uuid"

14365123651274
--9431149156168
content-disposition: form-data; name="x-amz-meta-tag"

Some,Tag,For,Picture
--9431149156168
content-disposition: form-data; name="AWSAccessKeyId"

AKIAIOSFODNN7EXAMPLE
--9431149156168
content-disposition: form-data; name="Policy"

eyAiZXhwaXJhdGlvbiI6ICIyMDA3LTEyLTAxVDEyOjAwOjAwLjAwMFoiLAogICJjb25kaXRpb25zIjogWwogICAgeyJidWNrZXQiOiAiam9obnNtaXRoIn0sCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci9lcmljLyJdLAogICAgeyJhY2wiOiAicHVibGljLXJlYWQifSwKICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL2pvaG5zbWl0aC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwKICAgIFsic3RhcnRzLXdpdGgiLCAiJENvbnRlbnQtVHlwZSIsICJpbWFnZS8iXSwKICAgIHsieC1hbXotbWV0YS11dWlkIjogIjE0MzY1MTIzNjUxMjc0In0sCiAgICBbInN0YXJ0cy13aXRoIiwgIiR4LWFtei1tZXRhLXRhZyIsICIiXQogIF0KfQo=
--9431149156168
content-disposition: form-data; name="Signature"

0RavWzkygo6QX9caELEqKi9kDbU=
--9431149156168
content-disposition: form-data; name="file"
content-type: image/jpeg

...file content...
--9431149156168
content-disposition: form-data; name="submit"

Upload to Amazon S3
--9431149156168--
"#;

        assert_eq!(
            expected,
            // Rust can't represent `\r` in a string literal, so we
            // replace `\r\n` with `\n` for comparison
            String::from_utf8(body.to_vec())
                .unwrap()
                .replace("\r\n", "\n")
        );
    }

    /// This test is inspired by <https://cloud.google.com/storage/docs/batch>
    #[test]
    fn test_multipart_mixed_gcs_batch_metadata() {
        let multipart = Multipart::new()
            .with_boundary("===============7330845974216740156==")
            .part(
                MixedPart::new("/storage/v1/b/example-bucket/o/obj1")
                    .method(Method::PATCH)
                    .part_header(
                        "content-id".parse().unwrap(),
                        "<b29c5de2-0db4-490b-b421-6a51b598bd22+1>".parse().unwrap(),
                    )
                    .header(
                        "content-type".parse().unwrap(),
                        "application/json".parse().unwrap(),
                    )
                    .header(
                        "accept".parse().unwrap(),
                        "application/json".parse().unwrap(),
                    )
                    .header("content-length".parse().unwrap(), "31".parse().unwrap())
                    .content(r#"{"metadata": {"type": "tabby"}}"#),
            )
            .part(
                MixedPart::new("/storage/v1/b/example-bucket/o/obj2")
                    .method(Method::PATCH)
                    .part_header(
                        "content-id".parse().unwrap(),
                        "<b29c5de2-0db4-490b-b421-6a51b598bd22+2>".parse().unwrap(),
                    )
                    .header(
                        "content-type".parse().unwrap(),
                        "application/json".parse().unwrap(),
                    )
                    .header(
                        "accept".parse().unwrap(),
                        "application/json".parse().unwrap(),
                    )
                    .header("content-length".parse().unwrap(), "32".parse().unwrap())
                    .content(r#"{"metadata": {"type": "tuxedo"}}"#),
            )
            .part(
                MixedPart::new("/storage/v1/b/example-bucket/o/obj3")
                    .method(Method::PATCH)
                    .part_header(
                        "content-id".parse().unwrap(),
                        "<b29c5de2-0db4-490b-b421-6a51b598bd22+3>".parse().unwrap(),
                    )
                    .header(
                        "content-type".parse().unwrap(),
                        "application/json".parse().unwrap(),
                    )
                    .header(
                        "accept".parse().unwrap(),
                        "application/json".parse().unwrap(),
                    )
                    .header("content-length".parse().unwrap(), "32".parse().unwrap())
                    .content(r#"{"metadata": {"type": "calico"}}"#),
            );

        let body = multipart.build();

        let expected = r#"--===============7330845974216740156==
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: <b29c5de2-0db4-490b-b421-6a51b598bd22+1>

PATCH /storage/v1/b/example-bucket/o/obj1 HTTP/1.1
content-type: application/json
accept: application/json
content-length: 31

{"metadata": {"type": "tabby"}}
--===============7330845974216740156==
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: <b29c5de2-0db4-490b-b421-6a51b598bd22+2>

PATCH /storage/v1/b/example-bucket/o/obj2 HTTP/1.1
content-type: application/json
accept: application/json
content-length: 32

{"metadata": {"type": "tuxedo"}}
--===============7330845974216740156==
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: <b29c5de2-0db4-490b-b421-6a51b598bd22+3>

PATCH /storage/v1/b/example-bucket/o/obj3 HTTP/1.1
content-type: application/json
accept: application/json
content-length: 32

{"metadata": {"type": "calico"}}
--===============7330845974216740156==--
"#;

        assert_eq!(
            expected,
            // Rust can't represent `\r` in a string literal, so we
            // replace `\r\n` with `\n` for comparison
            String::from_utf8(body.to_vec())
                .unwrap()
                .replace("\r\n", "\n")
        );
    }

    /// This test is inspired by <https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=azure-ad>
    #[test]
    fn test_multipart_mixed_azblob_batch_delete() {
        let multipart = Multipart::new()
            .with_boundary("batch_357de4f7-6d0b-4e02-8cd2-6361411a9525")
            .part(
                MixedPart::new("/container0/blob0")
                    .method(Method::DELETE)
                    .part_header("content-id".parse().unwrap(), "0".parse().unwrap())
                    .header(
                        "x-ms-date".parse().unwrap(),
                        "Thu, 14 Jun 2018 16:46:54 GMT".parse().unwrap(),
                    )
                    .header(
                        "authorization".parse().unwrap(),
                        "SharedKey account:G4jjBXA7LI/RnWKIOQ8i9xH4p76pAQ+4Fs4R1VxasaE="
                            .parse()
                            .unwrap(),
                    )
                    .header("content-length".parse().unwrap(), "0".parse().unwrap()),
            )
            .part(
                MixedPart::new("/container1/blob1")
                    .method(Method::DELETE)
                    .part_header("content-id".parse().unwrap(), "1".parse().unwrap())
                    .header(
                        "x-ms-date".parse().unwrap(),
                        "Thu, 14 Jun 2018 16:46:54 GMT".parse().unwrap(),
                    )
                    .header(
                        "authorization".parse().unwrap(),
                        "SharedKey account:IvCoYDQ+0VcaA/hKFjUmQmIxXv2RT3XwwTsOTHL39HI="
                            .parse()
                            .unwrap(),
                    )
                    .header("content-length".parse().unwrap(), "0".parse().unwrap()),
            )
            .part(
                MixedPart::new("/container2/blob2")
                    .method(Method::DELETE)
                    .part_header("content-id".parse().unwrap(), "2".parse().unwrap())
                    .header(
                        "x-ms-date".parse().unwrap(),
                        "Thu, 14 Jun 2018 16:46:54 GMT".parse().unwrap(),
                    )
                    .header(
                        "authorization".parse().unwrap(),
                        "SharedKey account:S37N2JTjcmOQVLHLbDmp2johz+KpTJvKhbVc4M7+UqI="
                            .parse()
                            .unwrap(),
                    )
                    .header("content-length".parse().unwrap(), "0".parse().unwrap()),
            );

        let body = multipart.build();

        let expected = r#"--batch_357de4f7-6d0b-4e02-8cd2-6361411a9525
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 0

DELETE /container0/blob0 HTTP/1.1
x-ms-date: Thu, 14 Jun 2018 16:46:54 GMT
authorization: SharedKey account:G4jjBXA7LI/RnWKIOQ8i9xH4p76pAQ+4Fs4R1VxasaE=
content-length: 0

--batch_357de4f7-6d0b-4e02-8cd2-6361411a9525
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 1

DELETE /container1/blob1 HTTP/1.1
x-ms-date: Thu, 14 Jun 2018 16:46:54 GMT
authorization: SharedKey account:IvCoYDQ+0VcaA/hKFjUmQmIxXv2RT3XwwTsOTHL39HI=
content-length: 0

--batch_357de4f7-6d0b-4e02-8cd2-6361411a9525
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 2

DELETE /container2/blob2 HTTP/1.1
x-ms-date: Thu, 14 Jun 2018 16:46:54 GMT
authorization: SharedKey account:S37N2JTjcmOQVLHLbDmp2johz+KpTJvKhbVc4M7+UqI=
content-length: 0

--batch_357de4f7-6d0b-4e02-8cd2-6361411a9525--
"#;

        assert_eq!(
            expected,
            // Rust can't represent `\r` in a string literal, so we
            // replace `\r\n` with `\n` for comparison
            String::from_utf8(body.to_vec())
                .unwrap()
                .replace("\r\n", "\n")
        );
    }
}
