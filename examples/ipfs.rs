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
use futures::StreamExt;
use log::info;
use opendal::{services::ipfs, Operator};

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"OpenDAL IPFS Example.

Available Environment Values:

- OPENDAL_IPFS_ROOT: root path in mutable file system, default: /
- OPENDAL_IPFS_ENDPOINT: ipfs endpoint, default: localhost:5001
"#
    );

    let mut builder = ipfs::Backend::build();
    // root must be absolute path in MFS.
    builder.root(&env::var("OPENDAL_IPFS_ROOT").unwrap_or_else(|_| "/".to_string()));
    builder.endpoint(
        &env::var("OPENDAL_IPFS_ENDPOINT").unwrap_or_else(|_| "http://localhost:5001".to_string()),
    );

    let op: Operator = Operator::new(builder.build()?);

    let path = "/file.txt";
    info!("try to write file: {}", &path);
    op.object(path).write("Hello, world!").await?;
    info!("write file successful!");

    let content = op.object(path).read().await?;
    info!("File content: {}", String::from_utf8_lossy(&content));

    let root = "/";
    let mut list = op.object(root).list().await?;
    info!("Listing entries in {}", &root);
    while let Some(res) = list.next().await {
        let item = res?;
        info!("Found entry: {}", item.path())
    }

    Ok(())
}
