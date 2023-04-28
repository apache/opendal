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

use bytes::{Bytes, BytesMut};
use http::{header::CONTENT_DISPOSITION, HeaderMap, HeaderName, HeaderValue};

/// Multipart is a builder for multipart/form-data.
#[derive(Debug)]
pub struct Multipart {
    boundary: String,
    parts: Vec<Part>,
}

impl Default for Multipart {
    fn default() -> Self {
        Self::new()
    }
}

impl Multipart {
    /// Create a new multipart with random boundary.
    pub fn new() -> Self {
        Multipart {
            boundary: uuid::Uuid::new_v4().to_string(),
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
    pub fn part(mut self, part: Part) -> Self {
        self.parts.push(part);
        self
    }

    pub(crate) fn build(self) -> (String, Bytes) {
        let mut bs = BytesMut::new();

        // Write headers.
        for v in self.parts {
            // Write the first boundary
            bs.extend_from_slice(b"--");
            bs.extend_from_slice(self.boundary.as_bytes());
            bs.extend_from_slice(b"\r\n");

            bs.extend_from_slice(&v.build());
        }

        // Write the last boundary
        bs.extend_from_slice(b"--");
        bs.extend_from_slice(self.boundary.as_bytes());
        bs.extend_from_slice(b"--");
        bs.extend_from_slice(b"\r\n");

        (self.boundary, bs.freeze())
    }
}

/// Part is a builder for multipart/form-data part.
#[derive(Debug)]
pub struct Part {
    headers: HeaderMap,
    content: Bytes,
}

impl Part {
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

    pub(crate) fn build(self) -> Bytes {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use http::header::CONTENT_TYPE;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_multipart_basic() {
        let multipart = Multipart::new()
            .part(Part::new("foo").content(Bytes::from("bar")))
            .part(Part::new("hello").content(Bytes::from("world")));

        let (boundary, body) = multipart.build();

        let expected = format!(
            "--{boundary}\r\n\
             content-disposition: form-data; name=\"foo\"\r\n\
             \r\n\
             bar\r\n\
             --{boundary}\r\n\
             content-disposition: form-data; name=\"hello\"\r\n\
             \r\n\
             world\r\n\
             --{boundary}--\r\n",
        );

        assert_eq!(expected, String::from_utf8(body.to_vec()).unwrap());
    }

    /// This test is inspired from <https://docs.aws.amazon.com/AmazonS3/latest/userguide/HTTPPOSTExamples.html>
    #[test]
    fn test_multipart_s3_form_upload() {
        let multipart = Multipart::new()
            .with_boundary("9431149156168")
            .part(Part::new("key").content("user/eric/MyPicture.jpg"))
            .part(Part::new("acl").content("public-read"))
            .part(Part::new("success_action_redirect").content(
                "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/successful_upload.html",
            ))
            .part(Part::new("Content-Type").content("image/jpeg"))
            .part(Part::new("x-amz-meta-uuid").content("14365123651274"))
            .part(Part::new("x-amz-meta-tag").content("Some,Tag,For,Picture"))
            .part(Part::new("AWSAccessKeyId").content("AKIAIOSFODNN7EXAMPLE"))
            .part(Part::new("Policy").content("eyAiZXhwaXJhdGlvbiI6ICIyMDA3LTEyLTAxVDEyOjAwOjAwLjAwMFoiLAogICJjb25kaXRpb25zIjogWwogICAgeyJidWNrZXQiOiAiam9obnNtaXRoIn0sCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci9lcmljLyJdLAogICAgeyJhY2wiOiAicHVibGljLXJlYWQifSwKICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL2pvaG5zbWl0aC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwKICAgIFsic3RhcnRzLXdpdGgiLCAiJENvbnRlbnQtVHlwZSIsICJpbWFnZS8iXSwKICAgIHsieC1hbXotbWV0YS11dWlkIjogIjE0MzY1MTIzNjUxMjc0In0sCiAgICBbInN0YXJ0cy13aXRoIiwgIiR4LWFtei1tZXRhLXRhZyIsICIiXQogIF0KfQo="))
            .part(Part::new("Signature").content("0RavWzkygo6QX9caELEqKi9kDbU="))
            .part(Part::new("file").header(CONTENT_TYPE, "image/jpeg".parse().unwrap()).content("...file content...")).part(Part::new("submit").content("Upload to Amazon S3"));

        let (_, body) = multipart.build();

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
content-disposition: form-data; name="Content-Type"

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
}
