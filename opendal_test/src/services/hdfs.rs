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

use opendal::services::hdfs;
use opendal::Accessor;

/// In order to test s3 service, please set the following environment variables:
///
/// - `OPENDAL_HDFS_TEST=on`: set to `on` to enable the test.
/// - `OPENDAL_HDFS_ROOT=/path/to/dir`: set the root dir.
/// - `OPENDAL_HDFS_ENDPOINT=<endpoint>`: set the endpoint of the hdfs service.
pub async fn new() -> Result<Option<Arc<dyn Accessor>>> {
    dotenv::from_filename(".env").ok();

    if env::var("OPENDAL_HDFS_TEST").is_err() || env::var("OPENDAL_HDFS_TEST").unwrap() != "on" {
        return Ok(None);
    }

    let root = &env::var("OPENDAL_HDFS_ROOT").unwrap_or_else(|_| "/".to_string());
    let root = format!("{}{}/", root, uuid::Uuid::new_v4());

    let mut builder = hdfs::Backend::build();
    builder.root(&root);
    builder
        .endpoint(&env::var("OPENDAL_HDFS_ENDPOINT").expect("OPENDAL_HDFS_ENDPOINT must be set"));
    Ok(Some(builder.finish().await?))
}
