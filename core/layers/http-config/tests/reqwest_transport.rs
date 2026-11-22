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

use opendal_core::HttpConfig;
use opendal_core::HttpTransporter;
use opendal_core::raw::SignedDuration;
use opendal_http_transport_reqwest::ReqwestTransport;
use opendal_layer_http_config::HttpConfigLayer;

#[test]
fn test_new_succeeds_with_reqwest_transport() {
    HttpTransporter::install_default(ReqwestTransport::default());

    let config = HttpConfig {
        proxy: Some("http://127.0.0.1:7890".to_string()),
        no_proxy: Some("169.254.169.254,.internal.example.com".to_string()),
        connect_timeout: Some(SignedDuration::from_secs(5)),
        read_timeout: Some(SignedDuration::from_millis(1500)),
        timeout: Some(SignedDuration::from_secs(30)),
        user_agent: Some("opendal-test/1.0".to_string()),
    };
    let layer = HttpConfigLayer::new(config.clone()).unwrap();
    assert_eq!(layer.config().proxy, config.proxy);
}
