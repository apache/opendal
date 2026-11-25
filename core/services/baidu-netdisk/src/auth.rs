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

use http::{Request, header};
use opendal_core::*;

pub fn sign<T>(mut request: Request<T>, token: &str) -> Result<Request<T>> {
    let mut parts = request.uri().clone().into_parts();
    let mut query: Vec<&str> = request
        .uri()
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|v| !v.is_empty())
        .filter(|v| {
            form_urlencoded::parse(v.as_bytes())
                .next()
                .is_none_or(|(name, _)| name != "access_token")
        })
        .collect();
    let encoded = form_urlencoded::Serializer::new(String::new())
        .append_pair("access_token", token)
        .finish();
    query.push(&encoded);
    parts.path_and_query = Some(
        format!("{}?{}", request.uri().path(), query.join("&"))
            .parse()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to encode Baidu authentication",
                )
            })?,
    );
    *request.uri_mut() = http::Uri::from_parts(parts)
        .map_err(|_| Error::new(ErrorKind::Unexpected, "failed to build authenticated URI"))?;
    request.headers_mut().insert(
        header::USER_AGENT,
        http::HeaderValue::from_static("pan.baidu.com"),
    );
    Ok(request)
}
