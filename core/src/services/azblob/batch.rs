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

use http::StatusCode;

use super::error::parse_http_error;
use crate::raw::*;
use crate::*;

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
    use super::*;

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
