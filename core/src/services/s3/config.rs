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

use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

/// Config for Aws S3 and compatible services (including minio, digitalocean space, Tencent Cloud Object Storage(COS) and so on) support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct S3Config {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// default to `/` if not set.
    pub root: Option<String>,
    /// bucket name of this backend.
    ///
    /// required.
    #[serde(alias = "aws_bucket", alias = "aws_bucket_name", alias = "bucket_name")]
    pub bucket: String,
    /// is bucket versioning enabled for this bucket
    pub enable_versioning: bool,
    /// endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
    /// - Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
    /// - Aliyun OSS: `https://{region}.aliyuncs.com`
    /// - Tencent COS: `https://cos.{region}.myqcloud.com`
    /// - Minio: `http://127.0.0.1:9000`
    ///
    /// If user inputs endpoint without scheme like "s3.amazonaws.com", we
    /// will prepend "https://" before it.
    ///
    /// - If endpoint is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    /// - If still not set, default to `https://s3.amazonaws.com`.
    #[serde(
        alias = "aws_endpoint",
        alias = "aws_endpoint_url",
        alias = "endpoint_url"
    )]
    pub endpoint: Option<String>,
    /// Region represent the signing region of this endpoint. This is required
    /// if you are using the default AWS S3 endpoint.
    ///
    /// If using a custom endpoint,
    /// - If region is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[serde(alias = "aws_region")]
    pub region: Option<String>,

    /// access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[serde(alias = "aws_access_key_id")]
    pub access_key_id: Option<String>,
    /// secret_access_key of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    #[serde(alias = "aws_secret_access_key")]
    pub secret_access_key: Option<String>,
    /// session_token (aka, security token) of this backend.
    ///
    /// This token will expire after sometime, it's recommended to set session_token
    /// by hand.
    #[serde(alias = "aws_session_token", alias = "aws_token", alias = "token")]
    pub session_token: Option<String>,
    /// role_arn for this backend.
    ///
    /// If `role_arn` is set, we will use already known config as source
    /// credential to assume role with `role_arn`.
    pub role_arn: Option<String>,
    /// external_id for this backend.
    pub external_id: Option<String>,
    /// role_session_name for this backend.
    pub role_session_name: Option<String>,
    /// Disable config load so that opendal will not load config from
    /// environment.
    ///
    /// For examples:
    ///
    /// - envs like `AWS_ACCESS_KEY_ID`
    /// - files like `~/.aws/config`
    pub disable_config_load: bool,
    /// Disable load credential from ec2 metadata.
    ///
    /// This option is used to disable the default behavior of opendal
    /// to load credential from ec2 metadata, a.k.a, IMDSv2
    pub disable_ec2_metadata: bool,
    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    pub allow_anonymous: bool,
    /// server_side_encryption for this backend.
    ///
    /// Available values: `AES256`, `aws:kms`.
    #[serde(alias = "aws_server_side_encryption")]
    pub server_side_encryption: Option<String>,
    /// server_side_encryption_aws_kms_key_id for this backend
    ///
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is not set, S3 will use aws managed kms key to encrypt data.
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is a valid kms key id, S3 will use the provided kms key to encrypt data.
    /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be
    ///   returned.
    /// - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id`
    ///   is a noop.
    #[serde(alias = "aws_sse_kms_key_id")]
    pub server_side_encryption_aws_kms_key_id: Option<String>,
    /// server_side_encryption_customer_algorithm for this backend.
    ///
    /// Available values: `AES256`.
    pub server_side_encryption_customer_algorithm: Option<String>,
    /// server_side_encryption_customer_key for this backend.
    ///
    /// Value: BASE64-encoded key that matches algorithm specified in
    /// `server_side_encryption_customer_algorithm`.
    #[serde(alias = "aws_sse_customer_key_base64")]
    pub server_side_encryption_customer_key: Option<String>,
    /// Set server_side_encryption_customer_key_md5 for this backend.
    ///
    /// Value: MD5 digest of key specified in `server_side_encryption_customer_key`.
    pub server_side_encryption_customer_key_md5: Option<String>,
    /// default storage_class for this backend.
    ///
    /// Available values:
    /// - `DEEP_ARCHIVE`
    /// - `GLACIER`
    /// - `GLACIER_IR`
    /// - `INTELLIGENT_TIERING`
    /// - `ONEZONE_IA`
    /// - `EXPRESS_ONEZONE`
    /// - `OUTPOSTS`
    /// - `REDUCED_REDUNDANCY`
    /// - `STANDARD`
    /// - `STANDARD_IA`
    ///
    /// S3 compatible services don't support all of them
    pub default_storage_class: Option<String>,
    /// Enable virtual host style so that opendal will send API requests
    /// in virtual host style instead of path style.
    ///
    /// - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
    /// - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
    #[serde(
        alias = "aws_virtual_hosted_style_request",
        alias = "virtual_hosted_style_request"
    )]
    pub enable_virtual_host_style: bool,
    /// Set maximum batch operations of this backend.
    ///
    /// Some compatible services have a limit on the number of operations in a batch request.
    /// For example, R2 could return `Internal Error` while batch delete 1000 files.
    ///
    /// Please tune this value based on services' document.
    #[deprecated(
        since = "0.52.0",
        note = "Please use `delete_max_size` instead of `batch_max_operations`"
    )]
    pub batch_max_operations: Option<usize>,
    /// Set the maximum delete size of this backend.
    ///
    /// Some compatible services have a limit on the number of operations in a batch request.
    /// For example, R2 could return `Internal Error` while batch delete 1000 files.
    ///
    /// Please tune this value based on services' document.
    pub delete_max_size: Option<usize>,
    /// Disable stat with override so that opendal will not send stat request with override queries.
    ///
    /// For example, R2 doesn't support stat with `response_content_type` query.
    pub disable_stat_with_override: bool,
    /// Checksum Algorithm to use when sending checksums in HTTP headers.
    /// This is necessary when writing to AWS S3 Buckets with Object Lock enabled for example.
    ///
    /// Available options:
    /// - "crc32c"
    #[serde(alias = "aws_checksum_algorithm")]
    pub checksum_algorithm: Option<String>,
    /// Disable write with if match so that opendal will not send write request with if match headers.
    ///
    /// For example, Ceph RADOS S3 doesn't support write with if match.
    pub disable_write_with_if_match: bool,

    /// Enable write with append so that opendal will send write request with append headers.
    pub enable_write_with_append: bool,

    /// OpenDAL uses List Objects V2 by default to list objects.
    /// However, some legacy services do not yet support V2.
    /// This option allows users to switch back to the older List Objects V1.
    pub disable_list_objects_v2: bool,

    /// Indicates whether the client agrees to pay for the requests made to the S3 bucket.
    #[serde(alias = "aws_request_payer", alias = "request_payer")]
    pub enable_request_payer: bool,
}

impl Debug for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("S3Config");

        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region);

        d.finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_aliases() {
        // Test basic config with original field names
        let json1 = r#"{
            "bucket": "test-bucket",
            "access_key_id": "test-key",
            "secret_access_key": "test-secret",
            "region": "us-west-2",
            "endpoint": "https://s3.amazonaws.com",
            "session_token": "test-token"
        }"#;

        let config1: S3Config = serde_json::from_str(json1).unwrap();
        assert_eq!(config1.bucket, "test-bucket");
        assert_eq!(config1.access_key_id, Some("test-key".to_string()));
        assert_eq!(config1.secret_access_key, Some("test-secret".to_string()));
        assert_eq!(config1.region, Some("us-west-2".to_string()));
        assert_eq!(
            config1.endpoint,
            Some("https://s3.amazonaws.com".to_string())
        );
        assert_eq!(config1.session_token, Some("test-token".to_string()));

        // Test config with AWS-prefixed aliases
        let json2 = r#"{
            "aws_bucket": "test-bucket",
            "aws_access_key_id": "test-key",
            "aws_secret_access_key": "test-secret",
            "aws_region": "us-west-2",
            "aws_endpoint": "https://s3.amazonaws.com",
            "aws_session_token": "test-token"
        }"#;

        let config2: S3Config = serde_json::from_str(json2).unwrap();
        assert_eq!(config2.bucket, "test-bucket");
        assert_eq!(config2.access_key_id, Some("test-key".to_string()));
        assert_eq!(config2.secret_access_key, Some("test-secret".to_string()));
        assert_eq!(config2.region, Some("us-west-2".to_string()));
        assert_eq!(
            config2.endpoint,
            Some("https://s3.amazonaws.com".to_string())
        );
        assert_eq!(config2.session_token, Some("test-token".to_string()));

        // Test additional aliases
        let json3 = r#"{
            "bucket_name": "test-bucket",
            "token": "test-token",
            "endpoint_url": "https://s3.amazonaws.com",
            "virtual_hosted_style_request": true,
            "aws_checksum_algorithm": "crc32c",
            "request_payer": true
        }"#;

        let config3: S3Config = serde_json::from_str(json3).unwrap();
        assert_eq!(config3.bucket, "test-bucket");
        assert_eq!(config3.session_token, Some("test-token".to_string()));
        assert_eq!(
            config3.endpoint,
            Some("https://s3.amazonaws.com".to_string())
        );
        assert!(config3.enable_virtual_host_style);
        assert_eq!(config3.checksum_algorithm, Some("crc32c".to_string()));
        assert!(config3.enable_request_payer);

        // Test encryption aliases
        let json4 = r#"{
            "bucket": "test-bucket",
            "aws_server_side_encryption": "aws:kms",
            "aws_sse_kms_key_id": "test-kms-key",
            "aws_sse_customer_key_base64": "dGVzdC1jdXN0b21lci1rZXk="
        }"#;

        let config4: S3Config = serde_json::from_str(json4).unwrap();
        assert_eq!(config4.bucket, "test-bucket");
        assert_eq!(config4.server_side_encryption, Some("aws:kms".to_string()));
        assert_eq!(
            config4.server_side_encryption_aws_kms_key_id,
            Some("test-kms-key".to_string())
        );
        assert_eq!(
            config4.server_side_encryption_customer_key,
            Some("dGVzdC1jdXN0b21lci1rZXk=".to_string())
        );
    }
}
