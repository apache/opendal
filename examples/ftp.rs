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

//! Example for initiating a ftp backend.

use std::env;

use anyhow::Result;
use log::info;
use opendal::services::ftp;
use opendal::services::ftp::Builder;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    println!(
        r#"OpenDAL ftp Example.

Available Environment Values:
    - OPENDAL_FTP_ENDPOINT=endpoint     # required  
    - OPENDAL_FTP_Port=port      # default with 21
    - OPENDAL_FTP_ROOT=/path/to/dir/   # if not set, will be seen as "/ftp"
    - OPENDAL_FTP_USER=user    # default with empty string ""
    - OPENDAL_FTP_PWD=password    # default with empty string ""
    - OPENDAL_FTP_TLS=bool    # default with false
    "#
    );

    // Create fs backend builder.
    let mut builder: Builder = ftp::Builder::default();
    builder.root(&env::var("OPENDAL_FTP_ROOT").unwrap_or_else(|_| "/ftp".to_string()));
    // Set the root for ftp, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.endpoint(&env::var("OPENDAL_FTP_ENDPOINT").unwrap_or_else(|_| "127.0.0.1".to_string()));
    builder.user(&env::var("OPENDAL_FTP_USER").unwrap_or_else(|_| "".to_string()));
    builder.password(&env::var("OPENDAL_FTP_PWD").unwrap_or_else(|_| "".to_string()));
    builder.port(&env::var("OPENDAL_FTP_PORT").unwrap_or_else(|_| "21".to_string()));
    let tls = &env::var("OPENDAL_FTP_TLS").unwrap_or_else(|_| "false".to_string());

    builder.tls(match tls.as_str() {
        "Yes" => true,
        "No" => false,
        _ => false,
    });

    // Use `Operator` normally.
    let op: Operator = Operator::new(builder.build()?);

    let path = uuid::Uuid::new_v4().to_string();

    info!("try to create file: {}", &path);
    op.object(&path).create().await?;
    info!("create file successful!");

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

    info!("try to write to file: {}", &path);
    op.object(&path).write("write test").await?;
    info!("write to file successful!",);

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

    let dir = "/ftptestfolder/";
    info!("try to create directory: {}", &dir);
    op.object("/ftptestfolder/").create().await?;
    info!("create folder successful!",);

    info!("try to delete directory: {}", &dir);
    op.object(&dir).delete().await?;
    info!("delete directory successful");

    Ok(())
}
