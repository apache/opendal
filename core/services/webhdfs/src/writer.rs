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

use std::sync::Arc;

use bytes::Buf;
use http::StatusCode;
use uuid::Uuid;

use super::core::parse_error;
use super::core::{ErrorContext, WebhdfsCore};
use crate::message::{BooleanResp, FileStatusWrapper};
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

pub type WebhdfsWriters =
    TwoWays<oio::BlockWriter<WebhdfsWriter>, oio::AppendWriter<WebhdfsWriter>>;

pub struct WebhdfsWriter {
    core: Arc<WebhdfsCore>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
}

impl WebhdfsWriter {
    pub fn new(core: Arc<WebhdfsCore>, ctx: OperationContext, op: OpWrite, path: String) -> Self {
        WebhdfsWriter {
            core,
            ctx,
            op,
            path,
        }
    }
}

impl oio::BlockWrite for WebhdfsWriter {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .webhdfs_create_object(&self.ctx, &self.path, Some(size), &self.op, body)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(MetadataBuilder::unknown().build()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Create")),
                resp,
            )),
        }
    }

    async fn write_block(&self, block_id: Uuid, size: u64, body: Buffer) -> Result<()> {
        let Some(ref atomic_write_dir) = self.core.atomic_write_dir else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write multi is not supported when atomic is not set",
            ));
        };
        let resp = self
            .core
            .webhdfs_create_object(
                &self.ctx,
                &format!("{atomic_write_dir}{block_id}"),
                Some(size),
                &self.op,
                body,
            )
            .await?;

        let status = resp.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Create")),
                resp,
            )),
        }
    }

    async fn complete_block(&self, block_ids: Vec<Uuid>) -> Result<Metadata> {
        let Some(ref atomic_write_dir) = self.core.atomic_write_dir else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write multi is not supported when atomic is not set",
            ));
        };
        let first_block_id = format!("{}{}", atomic_write_dir, block_ids[0].clone());
        if block_ids.len() >= 2 {
            let sources: Vec<String> = block_ids[1..]
                .iter()
                .map(|s| format!("{atomic_write_dir}{s}"))
                .collect();
            // concat blocks
            let resp = self
                .core
                .webhdfs_concat(&self.ctx, &first_block_id, sources)
                .await?;

            let status = resp.status();
            if status != StatusCode::OK {
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("Concat")),
                    resp,
                ));
            }
        }
        // Ensure parent directory exists before deleting the target or renaming,
        // because WebHDFS op=RENAME does not auto-create parent directories.
        let parent = get_parent(&self.path);
        if parent != "/" {
            let resp = self.core.webhdfs_create_dir(&self.ctx, parent).await?;
            match resp.status() {
                StatusCode::CREATED | StatusCode::OK => {
                    let bs = resp.into_body();
                    let resp = serde_json::from_reader::<_, BooleanResp>(bs.reader())
                        .map_err(new_json_deserialize_error)?;
                    if !resp.boolean {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "webhdfs create parent dir failed",
                        ));
                    }
                }
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("Mkdirs")),
                        resp,
                    ));
                }
            }
        }

        // delete the path file
        let resp = self
            .core
            .webhdfs_delete(&self.ctx, &self.path, false)
            .await?;

        let status = resp.status();
        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("Delete")),
                resp,
            ));
        }

        // rename concat file to path
        let resp = self
            .core
            .webhdfs_rename_object(&self.ctx, &first_block_id, &self.path)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp = serde_json::from_reader::<_, BooleanResp>(bs.reader())
                    .map_err(new_json_deserialize_error)?;
                if resp.boolean {
                    Ok(MetadataBuilder::unknown().build())
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "webhdfs rename object failed",
                    ))
                }
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Rename")),
                resp,
            )),
        }
    }

    async fn abort_block(&self, block_ids: Vec<Uuid>) -> Result<()> {
        let Some(ref atomic_write_dir) = self.core.atomic_write_dir else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write multi is not supported when atomic is not set",
            ));
        };
        for block_id in block_ids {
            let resp = self
                .core
                .webhdfs_delete(&self.ctx, &format!("{atomic_write_dir}{block_id}"), false)
                .await?;
            match resp.status() {
                StatusCode::OK => {}
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("Delete")),
                        resp,
                    ));
                }
            }
        }
        Ok(())
    }
}

impl oio::AppendWrite for WebhdfsWriter {
    async fn offset(&self) -> Result<u64> {
        let resp = self
            .core
            .webhdfs_get_file_status(&self.ctx, &self.path)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let file_status = serde_json::from_reader::<_, FileStatusWrapper>(bs.reader())
                    .map_err(new_json_deserialize_error)?
                    .file_status;

                Ok(file_status.length)
            }
            StatusCode::NOT_FOUND => {
                let resp = self
                    .core
                    .webhdfs_create_object(&self.ctx, &self.path, None, &self.op, Buffer::new())
                    .await?;

                let status = resp.status();
                match status {
                    StatusCode::CREATED | StatusCode::OK => Ok(0),
                    _ => Err(parse_error(
                        ErrorContext::new(ServiceOperation("Create")),
                        resp,
                    )),
                }
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("GetFileStatus")),
                resp,
            )),
        }
    }

    async fn append(&self, _offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .webhdfs_append(&self.ctx, &self.path, size, body)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::OK => Ok(MetadataBuilder::unknown().build()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Append")),
                resp,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use asyncband::once::OnceCell;
    use http::{Request, Response};
    use oio::BlockWrite;

    use super::*;

    type RecordedRequests = Arc<Mutex<Vec<(String, String)>>>;

    struct SequenceTransport {
        requests: RecordedRequests,
        responses: Mutex<VecDeque<Response<Buffer>>>,
    }

    impl SequenceTransport {
        fn new(responses: Vec<Response<Buffer>>) -> (Self, RecordedRequests) {
            let requests = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    requests: requests.clone(),
                    responses: Mutex::new(VecDeque::from(responses)),
                },
                requests,
            )
        }
    }

    impl HttpTransport for SequenceTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests
                .lock()
                .unwrap()
                .push((req.method().to_string(), req.uri().to_string()));
            let resp = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request in SequenceTransport");
            let (parts, body) = resp.into_parts();
            let size = body.len() as u64;
            Ok(Response::from_parts(
                parts,
                HttpBody::new(futures::stream::iter([Ok(body)]), Some(size)),
            ))
        }
    }

    fn writer_with_responses(
        path: &str,
        responses: Vec<Response<Buffer>>,
    ) -> (WebhdfsWriter, RecordedRequests) {
        let (transport, requests) = SequenceTransport::new(responses);
        let core = Arc::new(WebhdfsCore {
            info: ServiceInfo::new("webhdfs", "/", ""),
            capability: Capability {
                write: true,
                write_can_multi: true,
                ..Default::default()
            },
            root: "/".to_string(),
            endpoint: "http://127.0.0.1:9870".to_string(),
            user_name: None,
            delegation: None,
            root_checker: OnceCell::new(),
            atomic_write_dir: Some(".opendal_tmp/".to_string()),
            disable_list_batch: false,
        });
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));
        let writer = WebhdfsWriter::new(core, ctx, OpWrite::default(), path.to_string());
        (writer, requests)
    }

    #[tokio::test]
    async fn complete_block_creates_parent_dir_and_checks_rename_boolean() -> Result<()> {
        let block1 = Uuid::new_v4();
        let block2 = Uuid::new_v4();

        let (writer, requests) = writer_with_responses(
            "sub/dir/file.bin",
            vec![
                // CONCAT
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::new())
                    .unwrap(),
                // MKDIRS for sub/dir/
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::from(r#"{"boolean":true}"#))
                    .unwrap(),
                // DELETE existing target (returns false when absent)
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::from(r#"{"boolean":false}"#))
                    .unwrap(),
                // RENAME
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::from(r#"{"boolean":true}"#))
                    .unwrap(),
            ],
        );

        writer.complete_block(vec![block1, block2]).await?;

        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 4);
        assert!(captured[0].1.contains("op=CONCAT"));
        assert_eq!(
            captured[1],
            (
                "PUT".to_string(),
                "http://127.0.0.1:9870/webhdfs/v1/sub/dir/?op=MKDIRS&overwrite=true&noredirect=true"
                    .to_string()
            )
        );
        assert!(captured[2].1.contains("op=DELETE"));
        assert!(captured[3].1.contains("op=RENAME"));

        Ok(())
    }

    #[tokio::test]
    async fn complete_block_returns_error_when_rename_returns_false() {
        let block1 = Uuid::new_v4();

        let (writer, requests) = writer_with_responses(
            "sub/dir/file.bin",
            vec![
                // MKDIRS
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::from(r#"{"boolean":true}"#))
                    .unwrap(),
                // DELETE
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::from(r#"{"boolean":true}"#))
                    .unwrap(),
                // RENAME returns HTTP 200 OK with {"boolean": false}
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::from(r#"{"boolean":false}"#))
                    .unwrap(),
            ],
        );

        let err = writer.complete_block(vec![block1]).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn complete_block_aborts_before_delete_when_parent_mkdir_fails() {
        let block1 = Uuid::new_v4();

        let (writer, requests) = writer_with_responses(
            "sub/dir/file.bin",
            vec![
                // MKDIRS fails with ParentNotDirectoryException
                Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(Buffer::from(
                        r#"{"RemoteException":{"exception":"ParentNotDirectoryException","javaClassName":"org.apache.hadoop.fs.ParentNotDirectoryException","message":"Parent path is not a directory"}}"#,
                    ))
                    .unwrap(),
            ],
        );

        let err = writer.complete_block(vec![block1]).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotADirectory);
        // Must not call DELETE when parent directory creation failed.
        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].1.contains("op=MKDIRS"));
    }

    #[tokio::test]
    async fn complete_block_skips_mkdir_for_root_level_file() -> Result<()> {
        let block1 = Uuid::new_v4();

        let (writer, requests) = writer_with_responses(
            "file.bin",
            vec![
                // DELETE
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::from(r#"{"boolean":false}"#))
                    .unwrap(),
                // RENAME
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Buffer::from(r#"{"boolean":true}"#))
                    .unwrap(),
            ],
        );

        writer.complete_block(vec![block1]).await?;

        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert!(captured[0].1.contains("op=DELETE"));
        assert!(captured[1].1.contains("op=RENAME"));

        Ok(())
    }
}
