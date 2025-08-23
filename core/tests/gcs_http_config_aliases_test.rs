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

//! Test HTTP configuration aliases for GCS service (compatible with arrow-rs-object-store)

use opendal::services::GcsConfig;
use std::time::Duration;

#[test]
fn test_gcs_http1_only_alias() {
    // Test google_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "google_http1_only": true}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http1_only, true);
    assert_eq!(config.bucket, "test-bucket");

    // Test gcs_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "gcs_http1_only": true}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http1_only, true);
}

#[test]
fn test_gcs_http2_only_alias() {
    // Test google_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "google_http2_only": true}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http2_only, true);

    // Test gcs_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "gcs_http2_only": true}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http2_only, true);
}

#[test]
fn test_gcs_http2_keep_alive_interval_alias() {
    // Test google_ alias - using string duration format
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "google_http2_keep_alive_interval": {"secs": 30, "nanos": 0}}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(
        config.http2_keep_alive_interval,
        Some(Duration::from_secs(30))
    );

    // Test gcs_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "gcs_http2_keep_alive_interval": {"secs": 45, "nanos": 0}}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(
        config.http2_keep_alive_interval,
        Some(Duration::from_secs(45))
    );
}

#[test]
fn test_gcs_http2_keep_alive_timeout_alias() {
    // Test google_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "google_http2_keep_alive_timeout": {"secs": 5, "nanos": 0}}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(
        config.http2_keep_alive_timeout,
        Some(Duration::from_secs(5))
    );

    // Test gcs_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "gcs_http2_keep_alive_timeout": {"secs": 10, "nanos": 0}}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(
        config.http2_keep_alive_timeout,
        Some(Duration::from_secs(10))
    );
}

#[test]
fn test_gcs_http2_keep_alive_while_idle_alias() {
    // Test google_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "google_http2_keep_alive_while_idle": true}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http2_keep_alive_while_idle, true);

    // Test gcs_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "gcs_http2_keep_alive_while_idle": true}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http2_keep_alive_while_idle, true);
}

#[test]
fn test_gcs_http2_max_frame_size_alias() {
    // Test google_ alias
    let json = r#"{"bucket": "test-bucket", "allow_anonymous": true, "google_http2_max_frame_size": 32768}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http2_max_frame_size, Some(32768));

    // Test gcs_ alias
    let json =
        r#"{"bucket": "test-bucket", "allow_anonymous": true, "gcs_http2_max_frame_size": 16384}"#;
    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http2_max_frame_size, Some(16384));
}

#[test]
fn test_gcs_config_with_multiple_aliases() {
    // Test that multiple HTTP config aliases work together
    let json = r#"{
        "bucket": "test-bucket",
        "allow_anonymous": true,
        "google_http1_only": true,
        "gcs_http2_keep_alive_interval": {"secs": 30, "nanos": 0},
        "google_http2_keep_alive_while_idle": true,
        "gcs_http2_max_frame_size": 65536
    }"#;

    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.bucket, "test-bucket");
    assert_eq!(config.allow_anonymous, true);
    assert_eq!(config.http1_only, true);
    assert_eq!(
        config.http2_keep_alive_interval,
        Some(Duration::from_secs(30))
    );
    assert_eq!(config.http2_keep_alive_while_idle, true);
    assert_eq!(config.http2_max_frame_size, Some(65536));
}

#[test]
fn test_gcs_config_original_field_names() {
    // Test that original field names still work alongside aliases
    let json = r#"{
        "bucket": "test-bucket",
        "allow_anonymous": true,
        "http1_only": true,
        "http2_keep_alive_while_idle": true
    }"#;

    let config: GcsConfig = serde_json::from_str(json).unwrap();
    assert_eq!(config.http1_only, true);
    assert_eq!(config.http2_keep_alive_while_idle, true);
}
