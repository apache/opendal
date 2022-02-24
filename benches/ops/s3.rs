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

pub async fn init() -> Result<Operator> {
    if env::var("OPENDAL_S3_TEST").is_err() || env::var("OPENDAL_S3_TEST").unwrap() != "on" {
        return Err(anyhow::anyhow!("OPENDAL_S3_TEST is not set, skipping."));
    }

    Ok(Operator::new(
        s3::Backend::build()
            .root(&format!("/{}", uuid::Uuid::new_v4()))
            .bucket(&env::var("OPENDAL_S3_BUCKET")?)
            .endpoint(&env::var("OPENDAL_S3_ENDPOINT").unwrap_or_default())
            .credential(Credential::hmac(
                &env::var("OPENDAL_S3_ACCESS_KEY_ID")?,
                &env::var("OPENDAL_S3_SECRET_ACCESS_KEY")?,
            ))
            .finish()
            .await
            .expect("init s3"),
    ))
}
