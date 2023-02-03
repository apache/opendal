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

use anyhow::Result;
use log::info;
use opendal::services::ipfs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"OpenDAL IPFS Example.

Available Environment Values:

- OPENDAL_IPFS_ROOT: root path, like /ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ
- OPENDAL_IPFS_ENDPOINT: ipfs endpoint, like https://ipfs.io
"#
    );

    let mut builder = ipfs::Builder::default();
    // root must be absolute path in MFS.
    builder.root(&env::var("OPENDAL_IPFS_ROOT").unwrap_or_else(|_| "/".to_string()));
    builder.endpoint(
        &env::var("OPENDAL_IPFS_ENDPOINT").unwrap_or_else(|_| "https://ipfs.io".to_string()),
    );

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?.finish();
    info!("operator: {:?}", op);

    let path = "normal_file";

    info!("try to read file: {}", &path);
    let content = op.object(path).read().await?;
    info!(
        "read file successful, content: {}",
        String::from_utf8_lossy(&content)
    );

    info!("try to get file metadata: {}", &path);
    let meta = op.object(path).metadata().await?;
    info!(
        "get file metadata successful, size: {}B",
        meta.content_length()
    );

    Ok(())
}
