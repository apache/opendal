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

use http::StatusCode;

use super::core::parse_error;
use super::core::{ErrorContext, IpmfsCore};
use opendal_core::raw::*;
use opendal_core::*;

pub struct IpmfsDeleter {
    core: Arc<IpmfsCore>,
    ctx: OperationContext,
}

impl IpmfsDeleter {
    pub fn new(core: Arc<IpmfsCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::OneShotDelete for IpmfsDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let resp = self.core.ipmfs_rm(&self.ctx, &path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(()),
            _ => {
                let err = parse_error(ErrorContext::new(ServiceOperation("FilesRm")), resp);
                if err.kind() == ErrorKind::NotFound {
                    return Ok(());
                }
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response};

    use super::*;
    use crate::backend::IpmfsBuilder;

    struct StaticRmTransport {
        status: StatusCode,
        body: &'static str,
    }

    impl HttpTransport for StaticRmTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Buffer::from(self.body);
            let len = body.len() as u64;
            Ok(Response::builder()
                .status(self.status)
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(len)))
                .unwrap())
        }
    }

    fn operator_with_rm_response(status: StatusCode, body: &'static str) -> Result<Operator> {
        Ok(
            Operator::new(IpmfsBuilder::default().endpoint("http://127.0.0.1:5001"))?.with_context(
                OperationContext::new()
                    .with_http_transport(HttpTransporter::new(StaticRmTransport { status, body })),
            ),
        )
    }

    #[tokio::test]
    async fn test_delete_missing_path_is_idempotent() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let op = operator_with_rm_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"Message":"file does not exist","Code":0,"Type":"error"}"#,
            )?;
            op.delete("missing.txt").await?;
            Ok(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_delete_ok_succeeds() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let op = operator_with_rm_response(StatusCode::OK, "")?;
            op.delete("present.txt").await?;
            Ok(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_delete_other_errors_propagate() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let op = operator_with_rm_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"Message":"cannot remove directory","Code":0,"Type":"error"}"#,
            )?;
            let err = op.delete("dir").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(!err.is_temporary());

            let op_unavailable =
                operator_with_rm_response(StatusCode::SERVICE_UNAVAILABLE, "unavailable")?;
            let err = op_unavailable.delete("file.txt").await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_temporary());
            Ok(())
        })
        .await
        .expect("test timed out")
    }
}
