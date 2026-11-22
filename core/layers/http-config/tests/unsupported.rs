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

use http::Request;
use http::Response;
use opendal_core::Buffer;
use opendal_core::ErrorKind;
use opendal_core::HttpBody;
use opendal_core::HttpConfig;
use opendal_core::HttpTransport;
use opendal_core::HttpTransporter;
use opendal_core::Result;
use opendal_layer_http_config::HttpConfigLayer;

struct PlainTransport;

impl HttpTransport for PlainTransport {
    async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
        unreachable!("test transport is never used to send requests")
    }
}

#[test]
fn test_new_fails_when_transport_cannot_configure() {
    HttpTransporter::install_default(PlainTransport);

    let err = HttpConfigLayer::new(HttpConfig::default()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert_eq!(
        err.message(),
        "http transport does not support configuration"
    );
}
