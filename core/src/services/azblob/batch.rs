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

use bytes::BufMut;
use bytes::BytesMut;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::StatusCode;
use uuid::Uuid;

use super::error::parse_http_error;
use crate::raw::*;
use crate::*;

const AZURE_BATCH_LIMIT: usize = 256;

/// helper struct for batch requests
pub(crate) struct BatchDeleteRequestBuilder {
    url: String,
    sub_requests: Vec<Request<AsyncBody>>,
}

impl BatchDeleteRequestBuilder {
    /// create a new batch request builder
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            sub_requests: vec![],
        }
    }
    /// append request in batch
    ///
    /// # Note
    /// `sub_req` must have been signed by signer
    pub fn append(&mut self, sub_req: Request<AsyncBody>) -> &mut Self {
        debug_assert!(self.sub_requests.len() < AZURE_BATCH_LIMIT);
        self.sub_requests.push(sub_req);
        self
    }
    /// create an batch request, not signed by signer
    pub fn try_into_req(self) -> Result<Request<AsyncBody>> {
        let boundary = format!("opendal-{}", Uuid::new_v4());
        let mut body = BytesMut::new();

        let req_builder = Request::post(&self.url).header(
            CONTENT_TYPE,
            format!("multipart/mixed; boundary={}", boundary),
        );

        for (idx, req) in self.sub_requests.into_iter().enumerate() {
            let headers: String = req
                .headers()
                .iter()
                .map(|(k, v)| {
                    let (k, v) = (k.as_str(), v.to_str().unwrap());
                    format!("{}: {}", k, v)
                })
                .collect::<Vec<String>>()
                .join("\n");
            let path = req
                .uri()
                .path_and_query()
                .expect("delete request comes with no path")
                .to_string();

            let block = format!(
                r#"--{boundary}
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: {idx}

{} {} HTTP/1.1
{}

"#,
                req.method(),
                path,
                headers
            );
            // replace LF with CRLF, required by Azure Storage Blobs service.
            //
            // The Rust compiler converts all CRLF sequences to LF when reading source files
            // since 2019, so it is safe to convert here
            let block = block.replace('\n', "\r\n");
            body.put(block.as_bytes());
        }
        body.put(format!("--{}--", boundary).as_bytes());

        let content_length = body.len();
        req_builder
            .header(CONTENT_LENGTH, content_length)
            .body(AsyncBody::Bytes(body.freeze()))
            .map_err(new_request_build_error)
    }
}

pub(super) fn parse_batch_delete_response(
    boundary: &str,
    body: String,
    expect: Vec<String>,
) -> Result<Vec<(String, Result<RpDelete>)>> {
    let mut reps = Vec::with_capacity(expect.len());

    let mut resp_packs: Vec<&str> = body.trim().split(&format!("--{boundary}")).collect();
    if resp_packs.len() != (expect.len() + 2) {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "invalid batch delete response",
        ));
    }
    // drop the tail
    resp_packs.pop();
    for (resp_pack, name) in resp_packs[1..].iter().zip(expect.into_iter()) {
        // the http body use CRLF (\r\n) instead of LF (\n)
        // split the body at double CRLF
        let split: Vec<&str> = resp_pack.split("\r\n\r\n").collect();

        let header: Vec<&str> = split
            .get(1)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Empty item in batch response"))?
            .trim()
            .split_ascii_whitespace()
            .collect();

        let status_code = header
            .get(1)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "cannot find status code of HTTP response item!",
                )
            })?
            .parse::<u16>()
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    &format!("invalid status code: {:?}", e),
                )
            })?
            .try_into()
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    &format!("invalid status code: {:?}", e),
                )
            })?;

        let rep = match status_code {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => (name, Ok(RpDelete::default())),
            s => {
                let body = split.get(1).ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "Empty HTTP error response")
                })?;
                let err = parse_http_error(s, body)?;
                (name, Err(err))
            }
        };
        reps.push(rep)
    }
    Ok(reps)
}

#[cfg(test)]
mod test {
    use anyhow::anyhow;
    use anyhow::Result;
    use http::header::CONTENT_LENGTH;
    use http::header::CONTENT_TYPE;
    use http::Request;

    use super::BatchDeleteRequestBuilder;
    use crate::raw::AsyncBody;
    use crate::services::azblob::batch::parse_batch_delete_response;

    #[test]
    fn batch_delete_req_builder_test() -> Result<()> {
        let url = "https://test.blob.core.windows.net/test";
        let delete_url = "https://test.blob.core.windows.net/test/test.txt";
        let delete_req = Request::delete(delete_url)
            .header(CONTENT_LENGTH, 0)
            .body(AsyncBody::Empty)
            .expect("must success");

        let mut builder = BatchDeleteRequestBuilder::new(url);
        builder.append(delete_req);

        let req = builder.try_into_req().expect("must success");

        let (header, body) = req.into_parts();
        let content_type = header
            .headers
            .get(CONTENT_TYPE)
            .expect("expect header in request: CONTENT_TYPE: application/mixed.")
            .to_str()
            .unwrap();
        let boundary = content_type
            .split("boundary=")
            .collect::<Vec<&str>>()
            .get(1)
            .expect("get invalid CONTENT_TYPE header in response")
            .to_owned();

        let bs = match body {
            AsyncBody::Bytes(bs) => bs,
            _ => return Err(anyhow!("wrong body type")),
        };

        let s = String::from_utf8_lossy(&bs);
        let splits: Vec<&str> = s.split(&format!("--{}", boundary)).collect();
        assert_eq!(splits.len(), 3);

        let expect_body_str = r#"
Content-Type: application/http
Content-Transfer-Encoding: binary
Content-ID: 0

DELETE /test/test.txt HTTP/1.1
content-length: 0

"#
        .replace('\n', "\r\n");
        let actual_body_str = splits[1];
        assert_eq!(actual_body_str, expect_body_str);
        Ok(())
    }

    #[test]
    fn test_break_down_batch() {
        // the last item in batch is a mocked response.
        // if stronger validation is implemented for Azblob,
        // feel free to replace or remove it.
        let body = r#"--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed
Content-Type: application/http
Content-ID: 0

HTTP/1.1 202 Accepted
x-ms-delete-type-permanent: true
x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e284f
x-ms-version: 2018-11-09

--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed
Content-Type: application/http
Content-ID: 1

HTTP/1.1 202 Accepted
x-ms-delete-type-permanent: true
x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2851
x-ms-version: 2018-11-09

--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed
Content-Type: application/http
Content-ID: 2

HTTP/1.1 404 The specified blob does not exist.
x-ms-error-code: BlobNotFound
x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2852
x-ms-version: 2018-11-09
Content-Length: 216
Content-Type: application/xml

<?xml version="1.0" encoding="utf-8"?>
<Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.
RequestId:778fdc83-801e-0000-62ff-0334671e2852
Time:2018-06-14T16:46:54.6040685Z</Message></Error>

--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed
Content-Type: application/http
Content-ID: 3

HTTP/1.1 403 Request to blob forbidden
x-ms-error-code: BlobForbidden
x-ms-request-id: 778fdc83-801e-0000-62ff-0334671e2852
x-ms-version: 2018-11-09
Content-Length: 216
Content-Type: application/xml

<?xml version="1.0" encoding="utf-8"?>
<Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.
RequestId:778fdc83-801e-0000-62ff-0334671e2852
Time:2018-06-14T16:46:54.6040685Z</Message></Error>
--batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed--"#
            .replace('\n', "\r\n");

        let expected: Vec<_> = (0..=3).map(|n| format!("/to-del/{n}")).collect();
        let boundary = "batchresponse_66925647-d0cb-4109-b6d3-28efe3e1e5ed";
        let p =
            parse_batch_delete_response(boundary, body, expected.clone()).expect("must_success");
        assert_eq!(p.len(), expected.len());
        for (idx, ((del, rep), to_del)) in p.into_iter().zip(expected.into_iter()).enumerate() {
            assert_eq!(del, to_del);

            if idx != 3 {
                assert!(rep.is_ok());
            } else {
                assert!(rep.is_err());
            }
        }
    }
}
