// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Aws S3 and compatible services (including minio, digitalocean space and so on) support
//!
//! # Configuration
//!
//! - `root`: Set the work dir for backend.
//! - `bucket`: Set the container name for backend.
//! - `endpoint`: Set the endpoint for backend.
//! - `region`: Set the region for backend.
//! - `access_key_id`: Set the access_key_id for backend.
//! - `secret_access_key`: Set the secret_access_key for backend.
//! - `security_token`: Set the security_token for backend.
//! - `server_side_encryption`: Set the server_side_encryption for backend.
//! - `server_side_encryption_aws_kms_key_id`: Set the server_side_encryption_aws_kms_key_id for backend.
//! - `server_side_encryption_customer_algorithm`: Set the server_side_encryption_customer_algorithm for backend.
//! - `server_side_encryption_customer_key`: Set the server_side_encryption_customer_key for backend.
//! - `server_side_encryption_customer_key_md5`: Set the server_side_encryption_customer_key_md5 for backend.
//! - `disable_credential_loader`: Disable aws credential loader from env
//! - `enable_virtual_host_style`: Enable virtual host style.
//!
//! Refer to [`Builder`]'s public API docs for more information.
//!
//! # Environment
//!
//! - `OPENDAL_S3_ROOT`
//! - `OPENDAL_S3_BUCKET`
//! - `OPENDAL_S3_ENDPOINT`
//! - `OPENDAL_S3_REGION`
//! - `OPENDAL_S3_ACCESS_KEY_ID`
//! - `OPENDAL_S3_SECRET_ACCESS_KEY`
//! - `OPENDAL_S3_SECURITY_TOKEN`
//! - `OPENDAL_S3_SERVER_SIDE_ENCRYPTION`
//! - `OPENDAL_S3_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM`
//! - `OPENDAL_S3_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY`
//! - `OPENDAL_S3_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5`
//! - `OPENDAL_S3_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID`
//! - `OPENDAL_S3_ENABLE_VIRTUAL_HOST_STYLE`
//!
//! # Temporary security credentials
//!
//! OpenDAL now provides support for S3 temporary security credentials in IAM.
//!
//! The way to take advantage of this feature is to build your S3 backend with `Builder::security_token`.
//!
//! But OpenDAL will not refresh the temporary security credentials, please keep in mind to refresh those credentials in time.
//!
//! # Server Side Encryption
//!
//! OpenDAL provides full support of S3 Server Side Encryption(SSE) features.
//!
//! The easiest way to configure them is to use helper functions like
//!
//! - SSE-KMS: `server_side_encryption_with_aws_managed_kms_key`
//! - SSE-KMS: `server_side_encryption_with_customer_managed_kms_key`
//! - SSE-S3: `server_side_encryption_with_s3_key`
//! - SSE-C: `server_side_encryption_with_customer_key`
//!
//! If those functions don't fulfill need, low-level options are also provided:
//!
//! - Use service managed kms key
//!   - `server_side_encryption="aws:kms"`
//! - Use customer provided kms key
//!   - `server_side_encryption="aws:kms"`
//!   - `server_side_encryption_aws_kms_key_id="your-kms-key"`
//! - Use S3 managed key
//!   - `server_side_encryption="AES256"`
//! - Use customer key
//!   - `server_side_encryption_customer_algorithm="AES256"`
//!   - `server_side_encryption_customer_key="base64-of-your-aes256-key"`
//!   - `server_side_encryption_customer_key_md5="base64-of-your-aes256-key-md5"`
//!
//! After SSE have been configured, all requests send by this backed will attach those headers.
//!
//! Reference: [Protecting data using server-side encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html)
//!
//! # Example
//!
//! ## Via Environment
//!
//! Set environment correctly:
//!
//! ```shell
//! export OPENDAL_S3_ROOT=/path/to/dir/
//! export OPENDAL_S3_BUCKET=test
//! export OPENDAL_S3_ENDPOINT=https://s3.amazonaws.com
//! export OPENDAL_S3_ACCESS_KEY_ID=access_key_id
//! export OPENDAL_S3_SECRET_ACCESS_KEY=secret_access_key
//! ```
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op: Operator = Operator::from_env(Scheme::S3)?;
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Via Builder
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::s3;
//! use opendal::Accessor;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create s3 backend builder.
//!     let mut builder = s3::Builder::default();
//!     // Set the root for s3, all operations will happen under this root.
//!     //
//!     // NOTE: the root must be absolute path.
//!     builder.root("/path/to/dir");
//!     // Set the bucket name, this is required.
//!     builder.bucket("test");
//!     // Set the endpoint.
//!     //
//!     // For examples:
//!     // - "https://s3.amazonaws.com"
//!     // - "http://127.0.0.1:9000"
//!     // - "https://oss-ap-northeast-1.aliyuncs.com"
//!     // - "https://cos.ap-seoul.myqcloud.com"
//!     //
//!     // Default to "https://s3.amazonaws.com"
//!     builder.endpoint("https://s3.amazonaws.com");
//!     // Set the access_key_id and secret_access_key.
//!     //
//!     // OpenDAL will try load credential from the env.
//!     // If credential not set and no valid credential in env, OpenDAL will
//!     // send request without signing like anonymous user.
//!     builder.access_key_id("access_key_id");
//!     builder.secret_access_key("secret_access_key");
//!
//!     let op: Operator = Operator::new(builder.build()?);
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::Backend;
pub use backend::Builder;

mod dir_stream;
mod error;
