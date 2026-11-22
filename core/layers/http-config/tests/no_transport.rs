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

//! The default HTTP transport is process-wide and this test binary never
//! installs one.

use opendal_core::ErrorKind;
use opendal_core::HttpConfig;
use opendal_layer_http_config::HttpConfigLayer;

#[test]
fn test_new_fails_without_default_transport() {
    let err = HttpConfigLayer::new(HttpConfig::default()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    assert_eq!(err.message(), "default HTTP transport is not installed");
}

#[test]
fn test_new_rejects_no_proxy_without_proxy() {
    let config = HttpConfig {
        no_proxy: Some(".internal.example.com".to_string()),
        ..Default::default()
    };
    let err = HttpConfigLayer::new(config).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    assert_eq!(err.message(), "no_proxy requires proxy");
}
