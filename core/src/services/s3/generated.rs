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

use crate::raw::*;
use serde::Deserialize;
use std::fmt::Debug;
use std::fmt::Formatter;

/// Config for Aws S3 and compatible services (including minio, digitalocean space, Tencent Cloud Object Storage(COS) and so on) support.
#[derive(Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct S3Config {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,

    /// bucket name of this backend.
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
    /// If user inputs endpoint without scheme like "s3.amazonaws.com", we will prepend "https://" before it.
    pub endpoint: Option<String>,

    /// Region represent the signing region of this endpoint.
    /// This is required if you are using the default AWS S3 endpoint.
    ///
    /// If using a custom endpoint,
    /// - If region is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub region: Option<String>,

    /// access_key_id of this backend.
    pub access_key_id: Option<String>,

    /// secret_access_key of this backend.
    pub secret_access_key: Option<String>,

    /// security_token (aka, session token) of this backend.
    ///
    /// This token will expire after sometime, it's recommended not to set security_token by hand.
    pub security_token: Option<String>,

    /// role_arn of this backend.
    ///
    /// If `role_arn` is set, we will use already known config as source to assume role.
    pub role_arn: Option<String>,

    /// external_id of this backend.
    ///
    /// external_id is a unique identifier that is used by third parties to assume a role in their customers' accounts.
    pub external_id: Option<String>,

    /// Use this flag to make sure opendal don't load config from the environment.
    ///
    /// - envs like `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
    /// - files like `~/.aws/config`
    pub disable_config_load: bool,

    /// Use this flag to make sure opendal don't load credentials from the EC2 instance metadata service.
    pub disable_ec2_metadata: bool,

    /// Use this flag to allow anonymous access to the bucket.
    ///
    /// OpenDAL returns an permission error if credentials are not loaded by default. This flag allows users to send requests without signing.
    pub allow_anonymous: bool,

    /// server_side_encryption for this backend.
    pub server_side_encryption: Option<String>,

    /// server_side_encryption_aws_kms_key_id for this backend
    ///
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id` is not set, S3 will use aws managed kms key to encrypt data.
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id` is a valid kms key id, S3 will use the provided kms key to encrypt data.
    /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be returned.
    /// - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id` is a noop.
    pub server_side_encryption_aws_kms_key_id: Option<String>,

    /// server_side_encryption_customer_algorithm for this backend.
    pub server_side_encryption_customer_algorithm: Option<String>,

    /// server_side_encryption_customer_key for this backend.
    ///
    /// # Value
    ///
    /// base64 encoded key that matches algorithm specified in `server_side_encryption_customer_algorithm`.
    pub server_side_encryption_customer_key: Option<String>,

    /// server_side_encryption_customer_key_md5 for this backend.
    ///
    /// # Value
    ///
    /// MD5 digest of key specified in `server_side_encryption_customer_key`.
    pub server_side_encryption_customer_key_md5: Option<String>,

    /// default storage class for this backend.
    pub default_storage_class: Option<String>,

    /// Enable virtual host style so that opendal will send API requests in virtual host style instead of path style.
    ///
    /// - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
    /// - After enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
    ///
    /// Some s3 compatible services like `oss` require virtual host style.
    pub enable_virtual_host_style: bool,

    /// Maximum batch operations of this backend.
    ///
    /// Some compatible services have a limit on the number of operations in a batch request.
    /// For example, R2 could return `Internal Error` while batch delete 1000 files.
    ///
    /// Please tune this value based on services' document.
    pub batch_max_operations: Option<usize>,

    /// Disable stat with override so that opendal will not send stat request with override queries.
    ///
    /// For example, R2 doesn't support stat with `response_content_type` query.
    pub disable_stat_with_override: bool,
}
impl Debug for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("S3Config");

        d.field("root", &self.root);

        d.field("bucket", &self.bucket);

        d.field("endpoint", &self.endpoint);

        d.field("region", &self.region);

        d.field(
            "access_key_id",
            &self.access_key_id.as_deref().map(mask_secret),
        );

        d.field(
            "secret_access_key",
            &self.secret_access_key.as_deref().map(mask_secret),
        );

        d.field(
            "security_token",
            &self.security_token.as_deref().map(mask_secret),
        );

        d.field("role_arn", &self.role_arn);

        d.field("external_id", &self.external_id);

        d.field("disable_config_load", &self.disable_config_load);

        d.field("disable_ec2_metadata", &self.disable_ec2_metadata);

        d.field("allow_anonymous", &self.allow_anonymous);

        d.field("server_side_encryption", &self.server_side_encryption);

        d.field(
            "server_side_encryption_aws_kms_key_id",
            &self.server_side_encryption_aws_kms_key_id,
        );

        d.field(
            "server_side_encryption_customer_algorithm",
            &self.server_side_encryption_customer_algorithm,
        );

        d.field(
            "server_side_encryption_customer_key",
            &self
                .server_side_encryption_customer_key
                .as_deref()
                .map(mask_secret),
        );

        d.field(
            "server_side_encryption_customer_key_md5",
            &self
                .server_side_encryption_customer_key_md5
                .as_deref()
                .map(mask_secret),
        );

        d.field("default_storage_class", &self.default_storage_class);

        d.field("enable_virtual_host_style", &self.enable_virtual_host_style);

        d.field("batch_max_operations", &self.batch_max_operations);

        d.field(
            "disable_stat_with_override",
            &self.disable_stat_with_override,
        );

        d.finish()
    }
}
