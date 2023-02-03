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
//! Example for initiating a fs backend.

use std::env;

use anyhow::Result;
use log::info;
use opendal::services::hdfs;
use opendal::services::hdfs::Builder;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"OpenDAL HDFS Example.

Available Environment Values:

- OPENDAL_HDFS_ROOT: root path, default: /tmp
- OPENDAL_HDFS_NAME_NODE: name node for hdfs, default: default
    "#
    );

    // Create fs backend builder.
    let mut builder: Builder = hdfs::Builder::default();
    // Set the root for hdfs, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root(&env::var("OPENDAL_HDFS_ROOT").unwrap_or_else(|_| "/tmp".to_string()));
    // Set the name node for hdfs.
    //
    // Use `default` as default value.
    builder
        .name_node(&env::var("OPENDAL_HDFS_NAME_NODE").unwrap_or_else(|_| "default".to_string()));

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::create(builder)?.finish();

    let path = uuid::Uuid::new_v4().to_string();

    // Create an object handle to start operation on object.
    info!("try to write file: {}", &path);
    op.object(&path).write("Hello, world!").await?;
    info!("write file successful!");

    info!("try to read file: {}", &path);
    let content = op.object(&path).read().await?;
    info!(
        "read file successful, content: {}",
        String::from_utf8_lossy(&content)
    );

    info!("try to get file metadata: {}", &path);
    let meta = op.object(&path).metadata().await?;
    info!(
        "get file metadata successful, size: {}B",
        meta.content_length()
    );

    info!("try to delete file: {}", &path);
    op.object(&path).delete().await?;
    info!("delete file successful");

    Ok(())
}
