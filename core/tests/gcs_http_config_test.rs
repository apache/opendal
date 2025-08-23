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

//! Test HTTP configuration features for GCS service

use opendal::services::GcsConfig;
use opendal::Operator;
use std::time::Duration;

#[tokio::test]
async fn test_gcs_http1_only_configuration() {
    // Test Http1Only configuration
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    config.http1_only = true;

    let op = Operator::from_config(config).unwrap().finish();

    // The operator should be created successfully with HTTP/1 only configuration
    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_keep_alive_configuration() {
    // Test HTTP/2 keep-alive configurations
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    config.http2_keep_alive_interval = Some(Duration::from_secs(30));
    config.http2_keep_alive_timeout = Some(Duration::from_secs(5));
    config.http2_keep_alive_while_idle = true;

    let op = Operator::from_config(config).unwrap().finish();

    // The operator should be created successfully with HTTP/2 keep-alive configuration
    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_max_frame_size_configuration() {
    // Test HTTP/2 max frame size configuration
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    config.http2_max_frame_size = Some(32768); // 32KB

    let op = Operator::from_config(config).unwrap().finish();

    // The operator should be created successfully with HTTP/2 max frame size configuration
    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_only_configuration() {
    // Test Http2Only configuration
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    config.http2_only = true;

    let op = Operator::from_config(config).unwrap().finish();

    // The operator should be created successfully with HTTP/2 only configuration
    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_builder_http_configuration_methods() {
    use opendal::services::Gcs;

    // Test builder methods for HTTP configuration
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .http1_only()
            .http2_keep_alive_interval(Duration::from_secs(60))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_while_idle()
            .http2_max_frame_size(16384), // 16KB
    )
    .unwrap()
    .finish();

    // The operator should be created successfully using builder methods
    assert_eq!(op.info().name(), "test-bucket");
}
