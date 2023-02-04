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

//! Example for initiating a Google Cloud Storage backend.
use std::env;

use anyhow::Result;
use log::info;
use opendal::services::Gcs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"
    OpenDAL GCS example.

    Available Environment Variables:
    - OPENDAL_GCS_ENDPOINT: endpoint to GCS or GCS alike services, default is "https://storage.googleapis.com", optional
    - OPENDAL_GCS_BUCKET: bucket name, required
    - OPENDAL_GCS_ROOT: working directory of OpenDAL, default is "/", optional
    - OPENDAL_GCS_CREDENTIAL: OAUTH2 token for authentication, required
    "#
    );

    // create builder
    let mut builder = Gcs::default();

    // set the endpoint for GCS or GCS alike services
    builder.endpoint(&env::var("OPENDAL_GCS_ENDPOINT").unwrap_or_else(|_| "".to_string()));
    // set the bucket used for OpenDAL service
    builder.bucket(&env::var("OPENDAL_GCS_BUCKET").expect("env OPENDAL_GCS_BUCKET not set"));
    // set the working directory for OpenDAL service
    //
    // could be considered as a fixed prefix for all paths you past into the backend
    builder.root(&env::var("OPENDAL_GCS_ROOT").unwrap_or_else(|_| "".to_string()));
    // OAUTH2 base64 credentials
    builder.credential(
        &env::var("OPENDAL_GCS_CREDENTIAL").expect("env OPENDAL_GCS_CREDENTIAL not set"),
    );

    let op = Operator::create(builder)?.finish();
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
