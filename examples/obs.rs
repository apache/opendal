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

//! Example for initiating an obs backend.
use std::env;

use anyhow::Result;
use log::info;
use opendal::services::obs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"OpenDAL obs Example.

Available Environment Values:

- OPENDAL_OBS_ROOT: root path, default: /
- OPENDAL_OBS_BUCKET: bukcet name, required.
- OPENDAL_OBS_ENDPOINT: endpoint of obs service
- OPENDAL_OBS_ACCESS_KEY_ID: access key id of obs service, could be auto detected.
- OPENDAL_OBS_SECRET_ACCESS_KEY: secret access key of obs service, could be auto detected.
    "#
    );

    // Create s3 backend builder.
    let mut builder = obs::Builder::default();
    // Set the root for obs, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root(&env::var("OPENDAL_OBS_ROOT").unwrap_or_default());
    // Set the bucket name, this is required.
    builder.bucket(&env::var("OPENDAL_OBS_BUCKET").expect("env OPENDAL_OBS_BUCKET not set"));
    // Set the endpoint.
    //
    // For examples:
    // - `https://obs.cn-north-4.myhuaweicloud.com`
    // - `https://custom.obs.com`
    builder.endpoint(&env::var("OPENDAL_OBS_ENDPOINT").expect("env OPENDAL_OBS_BUCKET not set"));

    // Set the credential.
    //
    // OpenDAL will try load credential from the env.
    // If credential not set and no valid credential in env, OpenDAL will
    // send request without signing like anonymous user.
    builder.access_key_id(
        &env::var("OPENDAL_OBS_ACCESS_KEY_ID").expect("env OPENDAL_OBS_ACCESS_KEY_ID not set"),
    );
    builder.secret_access_key(
        &env::var("OPENDAL_OBS_SECRET_ACCESS_KEY")
            .expect("env OPENDAL_OBS_SECRET_ACCESS_KEY not set"),
    );

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder.build()?);
    info!("operator: {:?}", op);

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
    let meta = op.object(&path).stat().await?;
    info!(
        "get file metadata successful, size: {}B",
        meta.content_length()
    );

    info!("try to delete file: {}", &path);
    op.object(&path).delete().await?;
    info!("delete file successful");

    Ok(())
}
