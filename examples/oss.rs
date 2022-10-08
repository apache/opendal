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

//! Example for initiating a oss backend.
use std::env;

use anyhow::Result;
use futures::StreamExt;
use log::info;
use opendal::services::oss;
use opendal::services::oss::Builder;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"OpenDAL OSS Example.

Available Environment Values:

- OPENDAL_OSS_ROOT: root path, default: /
- OPENDAL_OSS_BUCKET: bukcet name, required.
- OPENDAL_OSS_ENDPOINT: endpoint of oss service, for example: https://oss-accelerate.aliyuncs.com
- OPENDAL_OSS_ACCESS_KEY_ID: access key id of oss service, could be auto detected.
- OPENDAL_OSS_SECRET_ACCESS_KEY: secret access key of oss service, could be auto detected.
    "#
    );

    // Create oss backend builder.
    let mut builder: Builder = oss::Builder::default();
    // Set the root for oss, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root(&env::var("OPENDAL_OSS_ROOT").unwrap_or_default());
    // Set the bucket name, this is required.
    builder.bucket(&env::var("OPENDAL_OSS_BUCKET").expect("env OPENDAL_OSS_BUCKET not set"));
    // Set the endpoint.
    //
    // For examples:
    // - "https://oss-ap-northeast-1.aliyuncs.com"
    // - "https://oss-cn-hangzhou.aliyuncs.com"
    //
    // Default to "https://oss-accelerate.aliyuncs.com"
    builder.endpoint(&env::var("OPENDAL_OSS_ENDPOINT").unwrap_or_default());
    // Set the credential.
    //
    // OpenDAL will try load credential from the env.
    // If credential not set and no valid credential in env, OpenDAL will
    // send request without signing like anonymous user.
    builder.access_key_id(
        &env::var("OPENDAL_OSS_ACCESS_KEY_ID").expect("env OPENDAL_OSS_ACCESS_KEY_ID not set"),
    );
    builder.access_key_secret(
        &env::var("OPENDAL_OSS_ACCESS_KEY_SECRET")
            .expect("env OPENDAL_OSS_ACCESS_KEY_SECRET not set"),
    );

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder.build()?);
    info!("operator: {:?}", op);

    let dir = uuid::Uuid::new_v4().to_string();
    // let p = uuid::Uuid::new_v4().to_string();

    // let path = format!("{}/{}", dir, p);
    let path = format!("{}/<><<", dir);

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

    info!("try to list file under: {}/", &dir);
    let mut s = op.object(&(dir + "/")).list().await?;
    while let Some(p) = s.next().await {
        info!("listed: {}", p.unwrap().path());
    }
    info!("list file successful!");

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
