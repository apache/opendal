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

use anyhow::Result;
use opendal::credential::Credential;
use opendal::services::s3;
use opendal::Operator;

use super::BehaviorTest;

/// In order to test s3 service, please set the following environment variables:
///
/// - `OPENDAL_S3_TEST=on`: set to `on` to enable the test.
/// - `OPENDAL_S3_BUCKET=<bucket>`: set the bucket name.
/// - `OPENDAL_S3_ENDPOINT=<endpoint>`: set the endpoint of the s3 service.
/// - `OPENDAL_S3_ACCESS_KEY_ID=<access_key_id>`: set the access key id.
/// - `OPENDAL_S3_SECRET_ACCESS_KEY=<secret_access_key>`: set the secret access key.
#[tokio::test]
async fn test_s3() -> Result<()> {
    dotenv::from_filename(".env").ok();

    if env::var("OPENDAL_S3_TEST").is_err() || env::var("OPENDAL_S3_TEST").unwrap() != "on" {
        println!("OPENDAL_S3_TEST is not set, skipping.");
        return Ok(());
    }

    let op = Operator::new(
        s3::Backend::build()
            .root(&format!("/{}", uuid::Uuid::new_v4()))
            .bucket(&env::var("OPENDAL_S3_BUCKET")?)
            .endpoint(&env::var("OPENDAL_S3_ENDPOINT").unwrap_or("".to_string()))
            .credential(Credential::hmac(
                &env::var("OPENDAL_S3_ACCESS_KEY_ID")?,
                &env::var("OPENDAL_S3_SECRET_ACCESS_KEY")?,
            ))
            .finish()
            .await?,
    );

    BehaviorTest::new(op).run().await
}
