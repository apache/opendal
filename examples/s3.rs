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

//! Example for initiating a s3 backend.
use std::env;
use std::sync::Arc;

use anyhow::Result;
use futures::AsyncReadExt;
use log::info;
use opendal::services::s3;
use opendal::services::s3::Builder;
use opendal::Accessor;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"OpenDAL S3 Example.

Available Environment Values:

RUST_LOG: log level
OPENDAL_S3_ROOT: root path, default: /
OPENDAL_S3_BUCKET: bukcet name, required.
OPENDAL_S3_ENDPOINT: endpoint of s3 service, default: https://s3.amazonaws.com
OPENDAL_S3_REGION: region of s3 service, could be auto detected.
OPENDAL_S3_ACCESS_KEY_ID: access key id of s3 service, could be auto detected.
OPENDAL_S3_SECRET_ACCESS_KEY: secret access key of s3 service, could be auto detected.

Examples:

OPENDAL_S3_BUCKET=opendal OPENDAL_S3_ACCESS_KEY_ID=minioadmin OPENDAL_S3_SECRET_ACCESS_KEY=minioadminx OPENDAL_S3_ENDPOINT=http://127.0.0.1:9900 OPENDAL_S3_REGION=test cargo run --example s3

    "#
    );

    // Create s3 backend builder.
    let mut builder: Builder = s3::Backend::build();
    // Set the root for s3, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root(&env::var("OPENDAL_S3_ROOT").unwrap_or_default());
    // Set the bucket name, this is required.
    builder.bucket(&env::var("OPENDAL_S3_BUCKET").expect("env OPENDAL_S3_BUCKET not set"));
    // Set the endpoint.
    //
    // For examples:
    // - "https://s3.amazonaws.com"
    // - "http://127.0.0.1:9000"
    // - "https://oss-ap-northeast-1.aliyuncs.com"
    // - "https://cos.ap-seoul.myqcloud.com"
    //
    // Default to "https://s3.amazonaws.com"
    builder.endpoint(
        &env::var("OPENDAL_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string()),
    );
    // Set the region in we have this env.
    if let Ok(region) = env::var("OPENDAL_S3_REGION") {
        builder.region(&region);
    }
    // Set the credential.
    //
    // OpenDAL will try load credential from the env.
    // If credential not set and no valid credential in env, OpenDAL will
    // send request without signing like anonymous user.
    builder.access_key_id(
        &env::var("OPENDAL_S3_ACCESS_KEY_ID").expect("env OPENDAL_S3_ACCESS_KEY_ID not set"),
    );
    builder.secret_access_key(
        &env::var("OPENDAL_S3_SECRET_ACCESS_KEY")
            .expect("env OPENDAL_S3_SECRET_ACCESS_KEY not set"),
    );
    // Build the `Accessor`.
    let accessor: Arc<dyn Accessor> = builder.finish().await?;

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(accessor);

    let path = uuid::Uuid::new_v4().to_string();

    // Create an object handle to start operation on object.
    info!("try to write file: {}", &path);
    op.object(&path).write_from_slice("Hello, world!").await?;
    info!("write file successful!");

    info!("try to read file: {}", &path);
    let mut content = String::new();
    op.object(&path)
        .reader()
        .await?
        .read_to_string(&mut content)
        .await?;
    info!("read file successful, content: {}", content);

    info!("try to get file metadata: {}", &path);
    let meta = op.object(&path).metadata().await?;
    info!(
        "get file metadata successful, size: {}B",
        meta.content_length()
    );

    info!("try to delete file: {}", &path);
    let _ = op.object(&path).delete().await?;
    info!("delete file successful");

    Ok(())
}
