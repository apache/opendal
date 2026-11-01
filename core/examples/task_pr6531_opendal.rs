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

const PAYLOAD: &[u8] = b"OpenDAL actual AWS multipart SSE-KMS request scope\n";
const PART_SIZE: usize = 5 * 1024 * 1024;

async fn execute() -> Result<Option<String>> {
    let args: Vec<_> = env::args().collect();
    assert_eq!(args.len(), 4, "mode, target, and source required");
    let mode = &args[1];
    let target = &args[2];
    let source = &args[3];
    assert!(target.starts_with("task-s3-multipart-sse-"));
    assert!(source.is_empty() || source.starts_with("task-s3-multipart-sse-"));
    assert!(env::var_os("AWS_REGION").is_none());
    assert!(env::var_os("AWS_DEFAULT_REGION").is_none());

    let mut builder = S3::default()
        .bucket(&env::var("OPENDAL_S3_BUCKET").expect("fixture bucket required"))
        .region(&env::var("OPENDAL_S3_REGION").expect("fixture region required"))
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
    let op = Operator::new(builder)?;
    let metadata = match mode.as_str() {
        "multipart" => {
            let mut writer = op.writer_with(target).chunk(PART_SIZE).await?;
            let result =
                async {
                    writer.write(vec![b'z'; PART_SIZE]).await.map_err(|error| {
                        error.with_context("task_phase", "multipart_first_write")
                    })?;
                    writer.write(PAYLOAD).await.map_err(|error| {
                        error.with_context("task_phase", "multipart_second_write")
                    })?;
                    writer
                        .close()
                        .await
                        .map_err(|error| error.with_context("task_phase", "multipart_close"))
                }
                .await;
            if result.is_err() {
                let _ = writer.abort().await;
            }
            result?
        }
        "multipart_copy" => op.copy_with(source, target).chunk(PART_SIZE).await?,
        _ => panic!("unsupported task mode"),
    };
    Ok(metadata.version().map(str::to_owned))
}

#[tokio::main]
async fn main() {
    match execute().await {
        Ok(version) => println!("VERSION\t{}", version.unwrap_or_default()),
        Err(error) => {
            // Inspect diagnostics in memory; print only fixed allowlisted labels.
            let description = error.to_string();
            let message = error.message();
            let phase = [
                "multipart_first_write",
                "multipart_second_write",
                "multipart_close",
            ]
            .into_iter()
            .find(|value| description.contains(value))
            .unwrap_or("unknown");
            let operation = [
                "UploadPartCopy",
                "UploadPart",
                "CompleteMultipartUpload",
                "CreateMultipartUpload",
            ]
            .into_iter()
            .find(|value| description.contains(&format!("service_operation: {value}")))
            .unwrap_or("unknown");
            let status = [
                "400", "403", "404", "409", "412", "429", "500", "502", "503", "504",
            ]
            .into_iter()
            .find(|value| description.contains(&format!("status: {value}")))
            .unwrap_or("unknown");
            let code = [
                "InvalidArgument",
                "InvalidRequest",
                "InvalidPart",
                "InvalidPartOrder",
                "SignatureDoesNotMatch",
                "XAmzContentSHA256Mismatch",
                "EntityTooSmall",
                "AccessDenied",
                "InvalidToken",
                "ExpiredToken",
                "NotImplemented",
                "NoSuchUpload",
                "MalformedXML",
                "InternalError",
                "SlowDown",
            ]
            .into_iter()
            .find(|value| message.contains(&format!("code: \"{value}\"")))
            .unwrap_or("other");
            eprintln!(
                "OPENDAL_ERROR kind={:?} stage={phase} s3_operation={operation} http_status={status} s3_code={code} mentions_sse={} mentions_bucket_key={}",
                error.kind(),
                message.contains("x-amz-server-side-encryption"),
                message.contains("bucket-key-enabled")
            );
            std::process::exit(1);
        }
    }
}
