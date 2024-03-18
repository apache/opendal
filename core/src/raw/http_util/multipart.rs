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

use std::collections::VecDeque;
use std::mem;
use std::str::FromStr;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use bytes::BytesMut;
use futures::stream;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::uri::PathAndQuery;
use http::HeaderMap;
use http::HeaderName;
use http::HeaderValue;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Uri;
use http::Version;

use super::new_request_build_error;
use super::AsyncBody;
use crate::raw::oio;
use crate::raw::oio::Stream;
use crate::raw::oio::Streamer;
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
    pub fn with_boundary(mut self, boundary: &str) -> Self {
        self.boundary = boundary.to_string();
        self
    }

    /// Insert a part into multipart.
    pub fn part(mut self, part: T) -> Self {
        self.parts.push(part);
        self
    }

    /// Into parts.
    pub fn into_parts(self) -> Vec<T> {
        self.parts
    }

    /// Parse a response with multipart body into Multipart.
    pub fn parse(mut self, bs: Bytes) -> Result<Self> {
        let s = String::from_utf8(bs.to_vec()).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "multipart response contains invalid utf-8 chars",
            )
            .set_source(err)
        })?;

        let parts = s
            .split(format!("--{}", self.boundary).as_str())
            .collect::<Vec<&str>>();

        for part in parts {
            if part.is_empty() || part.starts_with("--") {
                continue;
            }

            self.parts.push(T::parse(part)?);
        }

        Ok(self)
    }

    pub(crate) fn build(self) -> (u64, MultipartStream<T>) {
        let mut total_size = 0;

        let mut bs = BytesMut::new();
        bs.extend_from_slice(b"--");
        bs.extend_from_slice(self.boundary.as_bytes());
        bs.extend_from_slice(b"\r\n");

        let pre_part = bs.freeze();

        let mut parts = VecDeque::new();
        // Write headers.
        for v in self.parts.into_iter() {
            let (size, stream) = v.format();
            total_size += pre_part.len() as u64 + size;
            parts.push_back(stream);
        }

        // Write the last boundary
        let mut bs = BytesMut::new();
        bs.extend_from_slice(b"--");
        bs.extend_from_slice(self.boundary.as_bytes());
        bs.extend_from_slice(b"--");
        bs.extend_from_slice(b"\r\n");
        let final_part = bs.freeze();

        total_size += final_part.len() as u64;

        (
            total_size,
            MultipartStream {
                pre_part,
                pre_part_consumed: false,
                parts,
                final_part: Some(final_part),
            },
        )
    }

    /// Consume the input and generate a request with multipart body.
    ///
    /// This function will make sure content_type and content_length set correctly.
    pub fn apply(self, mut builder: http::request::Builder) -> Result<Request<AsyncBody>> {
        let boundary = self.boundary.clone();
        let (content_length, stream) = self.build();

        // Insert content type with correct boundary.
        builder = builder.header(
            CONTENT_TYPE,
            format!("multipart/{}; boundary={}", T::TYPE, boundary).as_str(),
        );
        // Insert content length with calculated size.
        builder = builder.header(CONTENT_LENGTH, content_length);

        builder
            .body(AsyncBody::Stream(Box::new(stream)))
            .map_err(new_request_build_error)
    }
}

pub struct MultipartStream<T: Part> {
    pre_part: Bytes,
    pre_part_consumed: bool,

    parts: VecDeque<T::STREAM>,

    final_part: Option<Bytes>,
}

impl<T: Part> Stream for MultipartStream<T> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if let Some(stream) = self.parts.front_mut() {
            if !self.pre_part_consumed {
                self.pre_part_consumed = true;
                return Poll::Ready(Some(Ok(self.pre_part.clone())));
            }
            return match ready!(stream.poll_next(cx)) {
                None => {
                    self.pre_part_consumed = false;
                    self.parts.pop_front();
                    return self.poll_next(cx);
                }
                Some(v) => Poll::Ready(Some(v)),
            };
        }

        if let Some(final_part) = self.final_part.take() {
            return Poll::Ready(Some(Ok(final_part)));
        }

        Poll::Ready(None)
    }
}

/// Part is a trait for multipart part.
pub trait Part: Sized + 'static {
    /// TYPE is the type of multipart.
    ///
    /// Current available types are: `form-data` and `mixed`
    const TYPE: &'static str;
    /// STREAM is the stream representation of this part which can be used in multipart body.
    type STREAM: Stream;

    /// format will generates the bytes.
    fn format(self) -> (u64, Self::STREAM);

    /// parse will parse the bytes into a part.
    fn parse(s: &str) -> Result<Self>;
}

/// FormDataPart is a builder for multipart/form-data part.
pub struct FormDataPart {
    headers: HeaderMap,

    content_length: u64,
    content: Streamer,
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
            content_length: 0,
            content: Box::new(oio::Cursor::new()),
        }
    }

    /// Insert a header into part.
    pub fn header(mut self, key: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Set the content for this part.
    pub fn content(mut self, content: impl Into<Bytes>) -> Self {
        let content = content.into();

        self.content_length = content.len() as u64;
        self.content = Box::new(oio::Cursor::from(content));
        self
    }

    /// Set the stream content for this part.
    pub fn stream(mut self, size: u64, content: Streamer) -> Self {
        self.content_length = size;
        self.content = content;
        self
    }
}

impl Part for FormDataPart {
    const TYPE: &'static str = "form-data";
    type STREAM = FormDataPartStream;

    fn format(self) -> (u64, FormDataPartStream) {
        let mut bs = BytesMut::new();

        // Building pre-content.
        for (k, v) in self.headers.iter() {
            // Trick!
            //
            // Seafile could not recognize header names like `content-disposition`
            // and requires to use `Content-Disposition`. So we hardcode the part
            // headers name here.
            match k.as_str() {
                "content-disposition" => {
                    bs.extend_from_slice("Content-Disposition".as_bytes());
                }
                _ => {
                    bs.extend_from_slice(k.as_str().as_bytes());
                }
            }
            bs.extend_from_slice(b": ");
            bs.extend_from_slice(v.as_bytes());
            bs.extend_from_slice(b"\r\n");
        }
        bs.extend_from_slice(b"\r\n");
        let bs = bs.freeze();

        // pre-content + content + post-content (b`\r\n`)
        let total_size = bs.len() as u64 + self.content_length + 2;

        (
            total_size,
            FormDataPartStream {
                pre_content: Some(bs),
                content: Some(self.content),
            },
        )
    }

    fn parse(_: &str) -> Result<Self> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "parse of form-data is not supported",
        ))
    }
}

pub struct FormDataPartStream {
    /// Including headers and the first `b\r\n`
    pre_content: Option<Bytes>,
    content: Option<Streamer>,
}

impl Stream for FormDataPartStream {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if let Some(pre_content) = self.pre_content.take() {
            return Poll::Ready(Some(Ok(pre_content)));
        }

        if let Some(stream) = self.content.as_mut() {
            return match ready!(stream.poll_next(cx)) {
                None => {
                    self.content = None;
                    Poll::Ready(Some(Ok(Bytes::from_static(b"\r\n"))))
                }
                Some(v) => Poll::Ready(Some(v)),
            };
        }

        Poll::Ready(None)
    }
}

/// MixedPart is a builder for multipart/mixed part.
pub struct MixedPart {
    part_headers: HeaderMap,

    /// Common
    version: Version,
    headers: HeaderMap,
    content_length: u64,
    content: Option<Streamer>,

    /// Request only
    method: Option<Method>,
    uri: Option<Uri>,

    /// Response only
    status_code: Option<StatusCode>,
}

impl MixedPart {
    /// Create a new mixed part with given uri.
    pub fn new(uri: &str) -> Self {
        let mut part_headers = HeaderMap::new();
        part_headers.insert(CONTENT_TYPE, "application/http".parse().unwrap());
        part_headers.insert("content-transfer-encoding", "binary".parse().unwrap());

        let uri = Uri::from_str(uri).expect("the uri used to build a mixed part must be valid");

        Self {
            part_headers,

            version: Version::HTTP_11,
            headers: HeaderMap::new(),
            content_length: 0,
            content: None,

            uri: Some(uri),
            method: None,

            status_code: None,
        }
    }

    /// Build a mixed part from a request.
    pub fn from_request(req: Request<AsyncBody>) -> Self {
        let mut part_headers = HeaderMap::new();
        part_headers.insert(CONTENT_TYPE, "application/http".parse().unwrap());
        part_headers.insert("content-transfer-encoding", "binary".parse().unwrap());

        let (parts, body) = req.into_parts();

        let (content_length, content) = match body {
            AsyncBody::Empty => (0, None),
            AsyncBody::Bytes(bs) => (
                bs.len() as u64,
                Some(Box::new(oio::Cursor::from(bs)) as Streamer),
            ),
            AsyncBody::Stream(stream) => {
                let len = parts
                    .headers
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .expect("the content length of a mixed part must be valid");
                (len, Some(stream))
            }
        };

        Self {
            part_headers,
            uri: Some(
                Uri::from_str(
                    parts
                        .uri
                        .path_and_query()
                        .unwrap_or(&PathAndQuery::from_static("/"))
                        .as_str(),
                )
                .expect("the uri used to build a mixed part must be valid"),
            ),
            version: parts.version,
            headers: parts.headers,
            content_length,
            content,

            method: Some(parts.method),
            status_code: None,
        }
    }

    /// Consume a mixed part to build a response.
    pub fn into_response(mut self) -> Response<oio::Buffer> {
        let mut builder = Response::builder();

        builder = builder.status(self.status_code.unwrap_or(StatusCode::OK));
        builder = builder.version(self.version);
        // Swap headers directly instead of copy the entire map.
        mem::swap(builder.headers_mut().unwrap(), &mut self.headers);

        // let body = if let Some(stream) = self.content {
        //     IncomingAsyncBody::new(stream, Some(self.content_length))
        // } else {
        //     IncomingAsyncBody::new(Box::new(oio::into_stream(stream::empty())), Some(0))
        // };

        // builder
        //     .body(body)
        //     .expect("mixed part must be valid response")
        todo!()
    }

    /// Insert a part header into part.
    pub fn part_header(mut self, key: HeaderName, value: HeaderValue) -> Self {
        self.part_headers.insert(key, value);
        self
    }

    /// Set the method for request in this part.
    pub fn method(mut self, method: Method) -> Self {
        self.method = Some(method);
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
        let content = content.into();

        self.content_length = content.len() as u64;
        self.content = Some(Box::new(oio::Cursor::from(content)));
        self
    }

    /// Set the stream content for this part.
    pub fn stream(mut self, size: u64, content: Streamer) -> Self {
        self.content_length = size;
        self.content = Some(content);
        self
    }
}

impl Part for MixedPart {
    const TYPE: &'static str = "mixed";
    type STREAM = MixedPartStream;

    fn format(self) -> (u64, Self::STREAM) {
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
        bs.extend_from_slice(
            self.method
                .as_ref()
                .expect("mixed part must be a valid request that contains method")
                .as_str()
                .as_bytes(),
        );
        bs.extend_from_slice(b" ");
        bs.extend_from_slice(
            self.uri
                .as_ref()
                .expect("mixed part must be a valid request that contains uri")
                .path()
                .as_bytes(),
        );
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
        bs.extend_from_slice(b"\r\n");

        let bs = bs.freeze();

        // pre-content + content + post-content;
        let mut total_size = bs.len() as u64;

        if self.content.is_some() {
            total_size += self.content_length + 2;
        }

        (
            total_size,
            MixedPartStream {
                pre_content: Some(bs),
                content: self.content,
            },
        )
    }

    /// TODO
    ///
    /// This is a simple implementation and have a lot of space to improve.
    fn parse(s: &str) -> Result<Self> {
        let parts = s.splitn(2, "\r\n\r\n").collect::<Vec<&str>>();
        let part_headers_content = parts[0];
        let http_response = parts.get(1).unwrap_or(&"");

        let mut part_headers = HeaderMap::new();
        for line in part_headers_content.lines() {
            let parts = line.splitn(2, ": ").collect::<Vec<&str>>();
            if parts.len() == 2 {
                let header_name = HeaderName::from_str(parts[0]).map_err(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "multipart response contains invalid part header name",
                    )
                    .set_source(err)
                })?;
                let header_value = parts[1].parse().map_err(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "multipart response contains invalid part header value",
                    )
                    .set_source(err)
                })?;

                part_headers.insert(header_name, header_value);
            }
        }

        let parts = http_response.split("\r\n\r\n").collect::<Vec<&str>>();
        let headers_content = parts[0];
        let body_content = parts.get(1).unwrap_or(&"");
        let body_bytes = Bytes::from(body_content.to_string());

        let status_line = headers_content.lines().next().unwrap_or("");
        let status_code = status_line
            .split_whitespace()
            .nth(1)
            .unwrap_or("")
            .parse::<u16>()
            .unwrap_or(200);

        let mut headers = HeaderMap::new();
        for line in headers_content.lines().skip(1) {
            let parts = line.splitn(2, ": ").collect::<Vec<&str>>();
            if parts.len() == 2 {
                let header_name = HeaderName::from_str(parts[0]).map_err(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "multipart response contains invalid part header name",
                    )
                    .set_source(err)
                })?;
                let header_value = parts[1].parse().map_err(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "multipart response contains invalid part header value",
                    )
                    .set_source(err)
                })?;

                headers.insert(header_name, header_value);
            }
        }

        Ok(Self {
            part_headers,
            version: Version::HTTP_11,
            headers,
            content_length: body_bytes.len() as u64,
            content: Some(Box::new(oio::Cursor::from(body_bytes))),

            method: None,
            uri: None,

            status_code: Some(StatusCode::from_u16(status_code).map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "multipart response contains invalid status code",
                )
                .set_source(err)
            })?),
        })
    }
}

pub struct MixedPartStream {
    /// Including headers and the first `b\r\n`
    pre_content: Option<Bytes>,
    content: Option<Streamer>,
}

impl Stream for MixedPartStream {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if let Some(pre_content) = self.pre_content.take() {
            return Poll::Ready(Some(Ok(pre_content)));
        }

        if let Some(stream) = self.content.as_mut() {
            return match ready!(stream.poll_next(cx)) {
                None => {
                    self.content = None;
                    Poll::Ready(Some(Ok(Bytes::from_static(b"\r\n"))))
                }
                Some(v) => Poll::Ready(Some(v)),
            };
        }

        Poll::Ready(None)
    }
}

#[cfg(test)]
mod tests {
    use http::header::CONTENT_TYPE;
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::raw::oio::StreamExt;

    #[tokio::test]
    async fn test_multipart_formdata_basic() -> Result<()> {
        let multipart = Multipart::new()
            .with_boundary("lalala")
            .part(FormDataPart::new("foo").content(Bytes::from("bar")))
            .part(FormDataPart::new("hello").content(Bytes::from("world")));

        let (size, body) = multipart.build();
        let bs = body.collect().await.unwrap();
        assert_eq!(size, bs.len() as u64);

        let expected = "--lalala\r\n\
             Content-Disposition: form-data; name=\"foo\"\r\n\
             \r\n\
             bar\r\n\
             --lalala\r\n\
             Content-Disposition: form-data; name=\"hello\"\r\n\
             \r\n\
             world\r\n\
             --lalala--\r\n";

        assert_eq!(Bytes::from(expected), bs);
        Ok(())
    }

    /// This test is inspired by <https://docs.aws.amazon.com/AmazonS3/latest/userguide/HTTPPOSTExamples.html>
    #[tokio::test]
    async fn test_multipart_formdata_s3_form_upload() -> Result<()> {
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

        let (size, body) = multipart.build();
        let bs = body.collect().await?;
        assert_eq!(size, bs.len() as u64);

        let expected = r#"--9431149156168
Content-Disposition: form-data; name="key"

user/eric/MyPicture.jpg
--9431149156168
Content-Disposition: form-data; name="acl"

public-read
--9431149156168
Content-Disposition: form-data; name="success_action_redirect"

https://awsexamplebucket1.s3.us-west-1.amazonaws.com/successful_upload.html
--9431149156168
Content-Disposition: form-data; name="content-type"

image/jpeg
--9431149156168
Content-Disposition: form-data; name="x-amz-meta-uuid"

14365123651274
--9431149156168
Content-Disposition: form-data; name="x-amz-meta-tag"

Some,Tag,For,Picture
--9431149156168
Content-Disposition: form-data; name="AWSAccessKeyId"

AKIAIOSFODNN7EXAMPLE
--9431149156168
Content-Disposition: form-data; name="Policy"

eyAiZXhwaXJhdGlvbiI6ICIyMDA3LTEyLTAxVDEyOjAwOjAwLjAwMFoiLAogICJjb25kaXRpb25zIjogWwogICAgeyJidWNrZXQiOiAiam9obnNtaXRoIn0sCiAgICBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci9lcmljLyJdLAogICAgeyJhY2wiOiAicHVibGljLXJlYWQifSwKICAgIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL2pvaG5zbWl0aC5zMy5hbWF6b25hd3MuY29tL3N1Y2Nlc3NmdWxfdXBsb2FkLmh0bWwifSwKICAgIFsic3RhcnRzLXdpdGgiLCAiJENvbnRlbnQtVHlwZSIsICJpbWFnZS8iXSwKICAgIHsieC1hbXotbWV0YS11dWlkIjogIjE0MzY1MTIzNjUxMjc0In0sCiAgICBbInN0YXJ0cy13aXRoIiwgIiR4LWFtei1tZXRhLXRhZyIsICIiXQogIF0KfQo=
--9431149156168
Content-Disposition: form-data; name="Signature"

0RavWzkygo6QX9caELEqKi9kDbU=
--9431149156168
Content-Disposition: form-data; name="file"
content-type: image/jpeg

...file content...
--9431149156168
Content-Disposition: form-data; name="submit"

Upload to Amazon S3
--9431149156168--
"#;

        assert_eq!(
            expected,
            // Rust can't represent `\r` in a string literal, so we
            // replace `\r\n` with `\n` for comparison
            String::from_utf8(bs.to_vec())
                .unwrap()
                .replace("\r\n", "\n")
        );

        Ok(())
    }

    /// This test is inspired by <https://cloud.google.com/storage/docs/batch>
    #[tokio::test]
    async fn test_multipart_mixed_gcs_batch_metadata() -> Result<()> {
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

        let (size, body) = multipart.build();
        let bs = body.collect().await?;
        assert_eq!(size, bs.len() as u64);

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
            String::from_utf8(bs.to_vec())
                .unwrap()
                .replace("\r\n", "\n")
        );

        Ok(())
    }

    /// This test is inspired by <https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=azure-ad>
    #[tokio::test]
    async fn test_multipart_mixed_azblob_batch_delete() -> Result<()> {
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

        let (size, body) = multipart.build();
        let bs = body.collect().await?;
        assert_eq!(size, bs.len() as u64);

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
            String::from_utf8(bs.to_vec())
                .unwrap()
                .replace("\r\n", "\n")
        );

        Ok(())
    }

    /// This test is inspired by <https://cloud.google.com/storage/docs/batch>
    #[test]
    fn test_multipart_mixed_gcs_batch_metadata_response() {
        let response = r#"--batch_pK7JBAk73-E=_AA5eFwv4m2Q=
Content-Type: application/http
Content-ID: <response-b29c5de2-0db4-490b-b421-6a51b598bd22+1>

HTTP/1.1 200 OK
ETag: "lGaP-E0memYDumK16YuUDM_6Gf0/V43j6azD55CPRGb9b6uytDYl61Y"
Content-Type: application/json; charset=UTF-8
Date: Mon, 22 Jan 2018 18:56:00 GMT
Expires: Mon, 22 Jan 2018 18:56:00 GMT
Cache-Control: private, max-age=0
Content-Length: 846

{"kind": "storage#object","id": "example-bucket/obj1/1495822576643790","metadata": {"type": "tabby"}}

--batch_pK7JBAk73-E=_AA5eFwv4m2Q=
Content-Type: application/http
Content-ID: <response-b29c5de2-0db4-490b-b421-6a51b598bd22+2>

HTTP/1.1 200 OK
ETag: "lGaP-E0memYDumK16YuUDM_6Gf0/91POdd-sxSAkJnS8Dm7wMxBSDKk"
Content-Type: application/json; charset=UTF-8
Date: Mon, 22 Jan 2018 18:56:00 GMT
Expires: Mon, 22 Jan 2018 18:56:00 GMT
Cache-Control: private, max-age=0
Content-Length: 846

{"kind": "storage#object","id": "example-bucket/obj2/1495822576643790","metadata": {"type": "tuxedo"}}

--batch_pK7JBAk73-E=_AA5eFwv4m2Q=
Content-Type: application/http
Content-ID: <response-b29c5de2-0db4-490b-b421-6a51b598bd22+3>

HTTP/1.1 200 OK
ETag: "lGaP-E0memYDumK16YuUDM_6Gf0/d2Z1F1_ZVbB1dC0YKM9rX5VAgIQ"
Content-Type: application/json; charset=UTF-8
Date: Mon, 22 Jan 2018 18:56:00 GMT
Expires: Mon, 22 Jan 2018 18:56:00 GMT
Cache-Control: private, max-age=0
Content-Length: 846

{"kind": "storage#object","id": "example-bucket/obj3/1495822576643790","metadata": {"type": "calico"}}

--batch_pK7JBAk73-E=_AA5eFwv4m2Q=--"#.replace('\n', "\r\n");

        let multipart: Multipart<MixedPart> = Multipart::new()
            .with_boundary("batch_pK7JBAk73-E=_AA5eFwv4m2Q=")
            .parse(Bytes::from(response))
            .unwrap();

        let part0_bs = Bytes::from_static(
            r#"{"kind": "storage#object","id": "example-bucket/obj1/1495822576643790","metadata": {"type": "tabby"}}"#.as_bytes());
        let part1_bs = Bytes::from_static(
            r#"{"kind": "storage#object","id": "example-bucket/obj2/1495822576643790","metadata": {"type": "tuxedo"}}"#
                .as_bytes()
        );
        let part2_bs = Bytes::from_static(
            r#"{"kind": "storage#object","id": "example-bucket/obj3/1495822576643790","metadata": {"type": "calico"}}"#
                .as_bytes()
        );

        assert_eq!(multipart.parts.len(), 3);

        assert_eq!(multipart.parts[0].part_headers, {
            let mut h = HeaderMap::new();
            h.insert("Content-Type", "application/http".parse().unwrap());
            h.insert(
                "Content-ID",
                "<response-b29c5de2-0db4-490b-b421-6a51b598bd22+1>"
                    .parse()
                    .unwrap(),
            );

            h
        });
        assert_eq!(multipart.parts[0].version, Version::HTTP_11);
        assert_eq!(multipart.parts[0].headers, {
            let mut h = HeaderMap::new();
            h.insert(
                "ETag",
                "\"lGaP-E0memYDumK16YuUDM_6Gf0/V43j6azD55CPRGb9b6uytDYl61Y\""
                    .parse()
                    .unwrap(),
            );
            h.insert(
                "Content-Type",
                "application/json; charset=UTF-8".parse().unwrap(),
            );
            h.insert("Date", "Mon, 22 Jan 2018 18:56:00 GMT".parse().unwrap());
            h.insert("Expires", "Mon, 22 Jan 2018 18:56:00 GMT".parse().unwrap());
            h.insert("Cache-Control", "private, max-age=0".parse().unwrap());
            h.insert("Content-Length", "846".parse().unwrap());

            h
        });
        assert_eq!(multipart.parts[0].content_length, part0_bs.len() as u64);
        assert_eq!(multipart.parts[0].uri, None);
        assert_eq!(multipart.parts[0].method, None);
        assert_eq!(
            multipart.parts[0].status_code,
            Some(StatusCode::from_u16(200).unwrap())
        );

        assert_eq!(multipart.parts[1].part_headers, {
            let mut h = HeaderMap::new();
            h.insert("Content-Type", "application/http".parse().unwrap());
            h.insert(
                "Content-ID",
                "<response-b29c5de2-0db4-490b-b421-6a51b598bd22+2>"
                    .parse()
                    .unwrap(),
            );

            h
        });
        assert_eq!(multipart.parts[1].version, Version::HTTP_11);
        assert_eq!(multipart.parts[1].headers, {
            let mut h = HeaderMap::new();
            h.insert(
                "ETag",
                "\"lGaP-E0memYDumK16YuUDM_6Gf0/91POdd-sxSAkJnS8Dm7wMxBSDKk\""
                    .parse()
                    .unwrap(),
            );
            h.insert(
                "Content-Type",
                "application/json; charset=UTF-8".parse().unwrap(),
            );
            h.insert("Date", "Mon, 22 Jan 2018 18:56:00 GMT".parse().unwrap());
            h.insert("Expires", "Mon, 22 Jan 2018 18:56:00 GMT".parse().unwrap());
            h.insert("Cache-Control", "private, max-age=0".parse().unwrap());
            h.insert("Content-Length", "846".parse().unwrap());

            h
        });
        assert_eq!(multipart.parts[1].content_length, part1_bs.len() as u64);
        assert_eq!(multipart.parts[1].uri, None);
        assert_eq!(multipart.parts[1].method, None);
        assert_eq!(
            multipart.parts[1].status_code,
            Some(StatusCode::from_u16(200).unwrap())
        );

        assert_eq!(multipart.parts[2].part_headers, {
            let mut h = HeaderMap::new();
            h.insert("Content-Type", "application/http".parse().unwrap());
            h.insert(
                "Content-ID",
                "<response-b29c5de2-0db4-490b-b421-6a51b598bd22+3>"
                    .parse()
                    .unwrap(),
            );

            h
        });
        assert_eq!(multipart.parts[2].version, Version::HTTP_11);
        assert_eq!(multipart.parts[2].headers, {
            let mut h = HeaderMap::new();
            h.insert(
                "ETag",
                "\"lGaP-E0memYDumK16YuUDM_6Gf0/d2Z1F1_ZVbB1dC0YKM9rX5VAgIQ\""
                    .parse()
                    .unwrap(),
            );
            h.insert(
                "Content-Type",
                "application/json; charset=UTF-8".parse().unwrap(),
            );
            h.insert("Date", "Mon, 22 Jan 2018 18:56:00 GMT".parse().unwrap());
            h.insert("Expires", "Mon, 22 Jan 2018 18:56:00 GMT".parse().unwrap());
            h.insert("Cache-Control", "private, max-age=0".parse().unwrap());
            h.insert("Content-Length", "846".parse().unwrap());

            h
        });
        assert_eq!(multipart.parts[2].content_length, part2_bs.len() as u64);
        assert_eq!(multipart.parts[2].uri, None);
        assert_eq!(multipart.parts[2].method, None);
        assert_eq!(
            multipart.parts[2].status_code,
            Some(StatusCode::from_u16(200).unwrap())
        );
    }
}
