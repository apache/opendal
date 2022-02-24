// Copyright 2021 Datafuse Labs.
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
use opendal::services::fs;
use opendal::Operator;

use super::BehaviorTest;

/// In order to test fs service, please set the following environment variables:
///
/// - `OPENDAL_FS_TEST=on`: set to `on` to enable the test.
/// - `OPENDAL_FS_ROOT=<path>`: set the root directory of the test.
#[tokio::test]
async fn test_fs() -> Result<()> {
    dotenv::from_filename(".env").ok();

    if env::var("OPENDAL_FS_TEST").is_err() || env::var("OPENDAL_FS_TEST").unwrap() != "on" {
        println!("OPENDAL_FS_TEST is not set, skipping.");
        return Ok(());
    }

    let op = Operator::new(
        fs::Backend::build()
            .root(&env::var("OPENDAL_FS_ROOT")?)
            .finish()
            .await?,
    );

    BehaviorTest::new(op).run().await
}
