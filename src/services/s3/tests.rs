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
use std::env;
use std::io::Result;
use std::sync::Arc;

use crate::Accessor;

/// In order to test s3 service, please set the following environment variables:
///
/// - `OPENDAL_S3_TEST=on`: set to `on` to enable the test.
/// - `OPENDAL_S3_ROOT=/path/to/dir`: set the root dir.
/// - `OPENDAL_S3_BUCKET=<bucket>`: set the bucket name.
/// - `OPENDAL_S3_ENDPOINT=<endpoint>`: set the endpoint of the s3 service.
/// - `OPENDAL_S3_ACCESS_KEY_ID=<access_key_id>`: set the access key id.
/// - `OPENDAL_S3_SECRET_ACCESS_KEY=<secret_access_key>`: set the secret access key.
pub async fn new() -> Result<Option<Arc<dyn Accessor>>> {
    if env::var("OPENDAL_S3_TEST").is_err() || env::var("OPENDAL_S3_TEST").unwrap() != "on" {
        return Ok(None);
    }

    let root = &env::var("OPENDAL_S3_ROOT").unwrap_or_else(|_| "/".to_string());
    let root = format!("/{}/{}", root, uuid::Uuid::new_v4());

    let mut builder = super::Backend::build();
    builder.root(&root);
    builder.bucket(&env::var("OPENDAL_S3_BUCKET").expect("OPENDAL_S3_BUCKET must set"));
    builder.endpoint(&env::var("OPENDAL_S3_ENDPOINT").unwrap_or_default());
    builder.access_key_id(&env::var("OPENDAL_S3_ACCESS_KEY_ID").unwrap_or_default());
    builder.secret_access_key(&env::var("OPENDAL_S3_SECRET_ACCESS_KEY").unwrap_or_default());
    builder
        .server_side_encryption(&env::var("OPENDAL_S3_SERVER_SIDE_ENCRYPTION").unwrap_or_default());
    builder.server_side_encryption_customer_algorithm(
        &env::var("OPENDAL_S3_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM").unwrap_or_default(),
    );
    builder.server_side_encryption_customer_key(
        &env::var("OPENDAL_S3_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY").unwrap_or_default(),
    );
    builder.server_side_encryption_customer_key_md5(
        &env::var("OPENDAL_S3_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5").unwrap_or_default(),
    );
    builder.server_side_encryption_aws_kms_key_id(
        &env::var("OPENDAL_S3_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID").unwrap_or_default(),
    );
    Ok(Some(builder.finish().await?))
}
