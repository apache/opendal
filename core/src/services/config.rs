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

// This file is generated by codegen/bin/genconfig.py.
// DO NOT MODIFY MANUALLY

#[cfg(feature = "services-s3")]
pub use s3::S3Config;

#[cfg(feature = "services-s3")]
pub mod s3 {
    use crate::raw::*;
    use serde::Deserialize;
    use std::fmt::Debug;
    use std::fmt::Formatter;

    /// Config for AWS S3 and compatible services support (MinIO, DigitalOcean Spaces, Tencent Cloud Object Storage, etc.)
    #[derive(Default, Deserialize)]
    #[serde(default)]
    #[non_exhaustive]
    pub struct S3Config {
        /// root of this backend.
        ///
        /// All operations will happen under this root.
        ///
        /// Default to `/` if not set.
        pub root: Option<String>,
        /// bucket name of this backend.
        ///
        /// Required.
        pub bucket: String,
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
        /// Default to `https://s3.amazonaws.com` if not set.
        pub endpoint: Option<String>,
        /// Region represent the signing region of this endpoint. This is required
        /// if you are using the default AWS S3 endpoint.
        ///
        /// If using a custom endpoint,
        /// - If region is set, we will take user's input first.
        /// - If not, we will try to load it from environment.
        pub region: Option<String>,
        /// access_key_id of this backend.
        ///
        /// - If access_key_id is set, we will take user's input first.
        /// - If not, we will try to load it from environment.
        pub access_key_id: Option<String>,
        /// secret_access_key of this backend.
        ///
        /// - If secret_access_key is set, we will take user's input first.
        /// - If not, we will try to load it from environment.
        pub secret_access_key: Option<String>,
        /// security_token (aka, session token) of this backend.
        ///
        /// This token will expire after sometime, it's recommended to set security_token by hand.
        pub security_token: Option<String>,
        /// role_arn for this backend.
        ///
        /// If `role_arn` is set, we will use already known config as source
        /// credential to assume role with `role_arn`.
        pub role_arn: Option<String>,
        /// external_id for this backend.
        pub external_id: Option<String>,
        /// Disable config load so that opendal will not load config from environment,
        /// e.g, envs like `AWS_ACCESS_KEY_ID` or files like `~/.aws/config`
        ///
        /// Required.
        pub disable_config_load: bool,
        /// Disable load credential from ec2 metadata.
        ///
        /// This option is used to disable the default behavior of opendal
        /// to load credential from ec2 metadata, a.k.a, IMDSv2
        ///
        /// Required.
        pub disable_ec2_metadata: bool,
        /// Allow anonymous will allow opendal to send request without signing
        /// when credential is not loaded.
        ///
        /// Required.
        pub allow_anonymous: bool,
        /// server_side_encryption for this backend.
        ///
        /// Available values:
        /// - `AES256`
        /// - `aws:kms`
        pub server_side_encryption: Option<String>,
        /// server_side_encryption_aws_kms_key_id for this backend
        ///
        /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
        /// is not set, S3 will use aws managed kms key to encrypt data.
        /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
        /// is a valid kms key id, S3 will use the provided kms key to encrypt data.
        /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be
        /// returned.
        /// - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id`
        /// is a noop.
        pub server_side_encryption_aws_kms_key_id: Option<String>,
        /// server_side_encryption_customer_algorithm for this backend.
        ///
        /// Available values:
        /// - `AES256`
        pub server_side_encryption_customer_algorithm: Option<String>,
        /// server_side_encryption_customer_key for this backend.
        ///
        /// # Value
        ///
        /// base64 encoded key that matches algorithm specified in
        /// `server_side_encryption_customer_algorithm`.
        pub server_side_encryption_customer_key: Option<String>,
        /// Set server_side_encryption_customer_key_md5 for this backend.
        ///
        /// # Value
        ///
        /// MD5 digest of key specified in `server_side_encryption_customer_key`.
        pub server_side_encryption_customer_key_md5: Option<String>,
        /// default_storage_class for this backend.
        ///
        /// S3 compatible services don't support all of available values.
        ///
        /// Available values:
        /// - `DEEP_ARCHIVE`
        /// - `GLACIER`
        /// - `GLACIER_IR`
        /// - `INTELLIGENT_TIERING`
        /// - `ONEZONE_IA`
        /// - `OUTPOSTS`
        /// - `REDUCED_REDUNDANCY`
        /// - `STANDARD`
        /// - `STANDARD_IA`
        pub default_storage_class: Option<String>,
        /// Enable virtual host style so that opendal will send API requests
        /// in virtual host style instead of path style.
        ///
        /// - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
        /// - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
        ///
        /// Required.
        pub enable_virtual_host_style: bool,
        /// Set maximum batch operations of this backend.
        ///
        /// Some compatible services have a limit on the number of operations in a batch request.
        /// For example, R2 could return `Internal Error` while batch delete 1000 files.
        ///
        /// Please tune this value based on services' document.
        pub batch_max_operations: Option<usize>,
        /// Disable stat with override so that opendal will not send stat request with override queries.
        ///
        /// For example, R2 doesn't support stat with `response_content_type` query.
        ///
        /// Required.
        pub disable_stat_with_override: bool,
    }

    impl Debug for S3Config {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut d = f.debug_struct("S3Config");

            d.field("root", &self.root)
                .field("bucket", &self.bucket)
                .field("endpoint", &self.endpoint)
                .field("region", &self.region)
                .field(
                    "access_key_id",
                    &self.access_key_id.as_deref().map(desensitize_secret),
                )
                .field(
                    "secret_access_key",
                    &self.secret_access_key.as_deref().map(desensitize_secret),
                )
                .field(
                    "security_token",
                    &self.security_token.as_deref().map(desensitize_secret),
                )
                .field("role_arn", &self.role_arn)
                .field("external_id", &self.external_id)
                .field("disable_config_load", &self.disable_config_load)
                .field("disable_ec2_metadata", &self.disable_ec2_metadata)
                .field("allow_anonymous", &self.allow_anonymous)
                .field("server_side_encryption", &self.server_side_encryption)
                .field(
                    "server_side_encryption_aws_kms_key_id",
                    &self.server_side_encryption_aws_kms_key_id,
                )
                .field(
                    "server_side_encryption_customer_algorithm",
                    &self.server_side_encryption_customer_algorithm,
                )
                .field(
                    "server_side_encryption_customer_key",
                    &self
                        .server_side_encryption_customer_key
                        .as_deref()
                        .map(desensitize_secret),
                )
                .field(
                    "server_side_encryption_customer_key_md5",
                    &self
                        .server_side_encryption_customer_key_md5
                        .as_deref()
                        .map(desensitize_secret),
                )
                .field("default_storage_class", &self.default_storage_class)
                .field("enable_virtual_host_style", &self.enable_virtual_host_style)
                .field("batch_max_operations", &self.batch_max_operations)
                .field(
                    "disable_stat_with_override",
                    &self.disable_stat_with_override,
                );

            d.finish_non_exhaustive()
        }
    }
}
