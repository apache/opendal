// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::env;

use opendal::Operator;
use opendal::Result;
use opendal::services::S3;

const PAYLOAD: &[u8] = b"OpenDAL PR6531 actual AWS SSE-KMS Bucket Key via OpenDAL\n";
const PART_SIZE: usize = 5 * 1024 * 1024;

async fn execute() -> Result<Option<String>> {
    let args: Vec<_> = env::args().collect();
    assert_eq!(args.len(), 5, "mode, flag, target, and source required");
    let mode = &args[1];
    let flag = &args[2];
    let target = &args[3];
    let source = &args[4];
    assert!(target.starts_with("pr6531-runtime-"));
    assert!(source.is_empty() || source.starts_with("pr6531-runtime-"));
    assert!(env::var_os("AWS_REGION").is_none());
    assert!(env::var_os("AWS_DEFAULT_REGION").is_none());

    let mut builder = S3::default()
        .bucket(&env::var("OPENDAL_S3_BUCKET").expect("fixture bucket required"))
        .default_region(&env::var("OPENDAL_S3_REGION").expect("fixture region required"))
        .root(&env::var("OPENDAL_S3_ROOT").unwrap_or_default())
        .access_key_id(&env::var("OPENDAL_S3_ACCESS_KEY_ID").expect("fixture key required"))
        .secret_access_key(&env::var("OPENDAL_S3_SECRET_ACCESS_KEY").expect("fixture key required"))
        .disable_config_load()
        .disable_ec2_metadata()
        .server_side_encryption("aws:kms");
    if let Ok(endpoint) = env::var("OPENDAL_S3_ENDPOINT") {
        builder = builder.endpoint(&endpoint);
    }
    if let Some(token) = env::var("OPENDAL_S3_SESSION_TOKEN")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| env::var("OPENDAL_S3_SECURITY_TOKEN").ok())
    {
        builder = builder.session_token(&token);
    }
    builder = match flag.as_str() {
        "omitted" => builder,
        "false" => builder.server_side_encryption_bucket_key_enabled(false),
        "true" => builder.server_side_encryption_bucket_key_enabled(true),
        _ => panic!("unsupported task flag"),
    };
    let op = Operator::new(builder)?;
    let metadata = match mode.as_str() {
        "oneshot" => op.write(target, PAYLOAD).await?,
        "multipart" => {
            let mut writer = op.writer_with(target).chunk(PART_SIZE).await?;
            let result = async {
                writer.write(vec![b'z'; PART_SIZE]).await?;
                writer.write(PAYLOAD).await?;
                writer.close().await
            }
            .await;
            if result.is_err() {
                let _ = writer.abort().await;
            }
            result?
        }
        "copy" => op.copy(source, target).await?,
        _ => panic!("unsupported task mode"),
    };
    Ok(metadata.version().map(str::to_owned))
}

#[tokio::main]
async fn main() {
    match execute().await {
        Ok(version) => println!("VERSION\t{}", version.unwrap_or_default()),
        Err(error) => {
            eprintln!("OPENDAL_ERROR kind={:?}", error.kind());
            std::process::exit(1);
        }
    }
}
