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

//! example for initiating a Redis backend

use std::env;

use anyhow::Result;
use log::info;
use opendal::services::Redis;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();
    println!(
        r#"
        OpenDAL redis example.

        Available Environment Variables:

        - OPENDAL_REDIS_ENDPOINT: network address of redis server, default is "tcp://127.0.0.1:6379"
        - OPENDAL_REDIS_ROOT: working directory of opendal, default is "/"
        - OPENDAL_REDIS_USERNAME: username for redis, default is no username
        - OPENDAL_REDIS_PASSWORD: password to log in. default is no password
        - OPENDAL_REDIS_DB: Redis db to use, default is 0.
        "#
    );

    // Create redis backend builder
    let mut builder = Redis::default();

    // Set the root, all operations will happen under this directory, or prefix, more accurately.
    //
    // NOTE: the root must be absolute path
    builder.root(&env::var("OPENDAL_REDIS_ROOT").unwrap_or_else(|_| "/".to_string()));

    // Set the endpoint, the address of remote redis server.
    builder.endpoint(
        &env::var("OPENDAL_REDIS_ENDPOINT").unwrap_or_else(|_| "tcp://127.0.0.1:6379".to_string()),
    );

    // Set the username
    builder.username(&env::var("OPENDAL_REDIS_USERNAME").unwrap_or_else(|_| "".to_string()));

    // Set the password
    builder.password(&env::var("OPENDAL_REDIS_PASSWORD").unwrap_or_else(|_| "".to_string()));

    // Set the db
    builder.db(env::var("OPENDAL_REDIS_DB")
        .map(|s| s.parse::<i64>().unwrap_or_default())
        .unwrap_or_else(|_| 0));

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
