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
use std::sync::Arc;

use opendal::credential::Credential;
use opendal::error::Result;
use opendal::services::azblob;
use opendal::Accessor;

/// In order to test azblob service, please set the following environment variables:
/// - `OPENDAL_AZBLOB_TEST=on`: set to `on` to enable the test.
/// - `OPENDAL_AZBLOB_ROOT=/path/to/dir`: set the root dir.
/// - `OPENDAL_AZBLOB_CONTAINER=<container>`: set the container name.
/// - `OPENDAL_AZBLOB_ENDPOINT=<endpoint>`: set the endpoint of the azblob service.
/// - `OPENDAL_AZBLOB_ACCOUNT_NAME=<account_name>`: set the account_name.
/// - `OPENDAL_AZBLOB_ACCOUNT_KEY=<account_key>`: set the account_key.
pub async fn new() -> Result<Option<Arc<dyn Accessor>>> {
    dotenv::from_filename(".env").ok();

    let root =
        &env::var("OPENDAL_AZBLOB_ROOT").unwrap_or_else(|_| format!("/{}", uuid::Uuid::new_v4()));

    let mut builder = azblob::Backend::build();

    builder
        .root(root)
        .container(
            &env::var("OPENDAL_AZBLOB_CONTAINER").expect("OPENDAL_AZBLOB_CONTAINER must set"),
        )
        .endpoint(&env::var("OPENDAL_AZBLOB_ENDPOINT").unwrap_or_default())
        .credential(Credential::hmac(
            &env::var("OPENDAL_AZBLOB_ACCOUNT_NAME").unwrap_or_default(),
            &env::var("OPENDAL_AZBLOB_ACCOUNT_KEY").unwrap_or_default(),
        ));

    Ok(Some(builder.finish().await?))
}
