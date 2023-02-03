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

//! Example for initiating a azblob backend.

use std::env;

use anyhow::Result;
use log::info;
use opendal::services::azblob;
use opendal::services::azblob::Builder;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"OpenDAL azblob example.

Available Environment Values:

- OPENDAL_AZBLOB_ROOT: root path, default: /
- OPENDAL_AZBLOB_CONTAINER: container name
- OPENDAL_AZBLOB_ENDPOINT: endpoint of your container
- OPENDAL_AZBLOB_ACCOUNT_NAME: account name
- OPENDAL_AZBLOB_ACCOUNT_KEY: account key

    "#
    );

    // Create fs backend builder.
    let mut builder: Builder = azblob::Builder::default();
    // Set the root, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root(&env::var("OPENDAL_AZBLOB_ROOT").unwrap_or_else(|_| "/".to_string()));
    // Set the container name
    builder.container(
        &env::var("OPENDAL_AZBLOB_CONTAINER").expect("env OPENDAL_AZBLOB_CONTAINER not set"),
    );
    // Set the endpoint
    //
    // For examples:
    // - "http://127.0.0.1:10000/devstoreaccount1"
    // - "https://accountname.blob.core.windows.net"
    builder.endpoint(
        &env::var("OPENDAL_AZBLOB_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:10000/devstoreaccount1".to_string()),
    );
    // Set the account_name and account_key.
    builder.account_name(
        &env::var("OPENDAL_AZBLOB_ACCOUNT_NAME").unwrap_or_else(|_| "devstoreaccount1".to_string()),
    );
    builder.account_key(
        &env::var("OPENDAL_AZBLOB_ACCOUNT_KEY")
            .unwrap_or_else(|_| "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_string()),
    );

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
