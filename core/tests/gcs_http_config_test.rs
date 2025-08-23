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
async fn test_gcs_http2_keep_alive_interval_configuration() {
    // Test HTTP/2 keep-alive interval configuration
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    config.http2_keep_alive_interval = Some(Duration::from_secs(30));

    let op = Operator::from_config(config).unwrap().finish();

    // The operator should be created successfully with HTTP/2 keep-alive interval configuration
    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_keep_alive_timeout_configuration() {
    // Test HTTP/2 keep-alive timeout configuration
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    config.http2_keep_alive_timeout = Some(Duration::from_secs(5));

    let op = Operator::from_config(config).unwrap().finish();

    // The operator should be created successfully with HTTP/2 keep-alive timeout configuration
    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_keep_alive_while_idle_configuration() {
    // Test HTTP/2 keep-alive while idle configuration
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    config.http2_keep_alive_while_idle = true;

    let op = Operator::from_config(config).unwrap().finish();

    // The operator should be created successfully with HTTP/2 keep-alive while idle configuration
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
async fn test_gcs_http1_only_builder_method() {
    use opendal::services::Gcs;

    // Test http1_only builder method
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .http1_only(),
    )
    .unwrap()
    .finish();

    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_only_builder_method() {
    use opendal::services::Gcs;

    // Test http2_only builder method
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .http2_only(),
    )
    .unwrap()
    .finish();

    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_keep_alive_interval_builder_method() {
    use opendal::services::Gcs;

    // Test http2_keep_alive_interval builder method
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .http2_keep_alive_interval(Duration::from_secs(60)),
    )
    .unwrap()
    .finish();

    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_keep_alive_timeout_builder_method() {
    use opendal::services::Gcs;

    // Test http2_keep_alive_timeout builder method
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .http2_keep_alive_timeout(Duration::from_secs(10)),
    )
    .unwrap()
    .finish();

    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_keep_alive_while_idle_builder_method() {
    use opendal::services::Gcs;

    // Test http2_keep_alive_while_idle builder method
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .http2_keep_alive_while_idle(),
    )
    .unwrap()
    .finish();

    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_http2_max_frame_size_builder_method() {
    use opendal::services::Gcs;

    // Test http2_max_frame_size builder method
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .http2_max_frame_size(16384), // 16KB
    )
    .unwrap()
    .finish();

    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_allow_http_configuration() {
    // Test allow_http configuration (default should be true)
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    assert_eq!(config.allow_http, true); // Default should be true

    config.allow_http = false;
    let op = Operator::from_config(config).unwrap().finish();
    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_randomize_addresses_configuration() {
    // Test randomize_addresses configuration (default should be true)
    let mut config = GcsConfig::default();
    config.bucket = "test-bucket".to_string();
    config.allow_anonymous = true;
    assert_eq!(config.randomize_addresses, true); // Default should be true

    config.randomize_addresses = false;
    let op = Operator::from_config(config).unwrap().finish();
    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_allow_http_builder_method() {
    use opendal::services::Gcs;

    // Test allow_http builder method
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .allow_http(false),
    )
    .unwrap()
    .finish();

    assert_eq!(op.info().name(), "test-bucket");
}

#[tokio::test]
async fn test_gcs_randomize_addresses_builder_method() {
    use opendal::services::Gcs;

    // Test randomize_addresses builder method
    let op = Operator::new(
        Gcs::default()
            .bucket("test-bucket")
            .allow_anonymous()
            .randomize_addresses(false),
    )
    .unwrap()
    .finish();

    assert_eq!(op.info().name(), "test-bucket");
}
