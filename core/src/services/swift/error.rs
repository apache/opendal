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

use bytes::Bytes;
use http::Response;
use http::StatusCode;
use scraper::Html;

use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

pub async fn parse_error(resp: Response<IncomingAsyncBody>) -> Result<Error> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    let (kind, retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::PRECONDITION_FAILED => (ErrorKind::ConditionNotMatch, false),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let message = parse_error_response(&bs);

    let mut err = Error::new(kind, &message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    Ok(err)
}

fn parse_error_response(resp: &Bytes) -> String {
    let html = Html::parse_document(match std::str::from_utf8(resp) {
        Ok(s) => s,
        Err(_) => return String::from_utf8_lossy(resp).to_string(),
    });
    let p_selector = scraper::Selector::parse("p").unwrap();
    let msg = html
        .select(&p_selector)
        .next()
        .map(|p| p.inner_html())
        .unwrap_or_else(|| String::from_utf8_lossy(resp).to_string());
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_response_test() -> Result<()> {
        let resp = Bytes::from(
            r#"
<html>
<h1>Not Found</h1>
<p>The resource could not be found.</p>

</html>
            "#,
        );

        let msg = parse_error_response(&resp);
        assert_eq!(msg, "The resource could not be found.".to_string(),);
        Ok(())
    }
}
