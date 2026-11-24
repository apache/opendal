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
use super::core::*;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

pub struct WebdavDeleter {
    core: Arc<WebdavCore>,
    ctx: OperationContext,
}

impl WebdavDeleter {
    pub fn new(core: Arc<WebdavCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::OneShotDelete for WebdavDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let resp = self.core.webdav_delete(&self.ctx, &path, &args).await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::ACCEPTED | StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND if args.if_match().is_some() => Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "delete precondition requires a live target",
            )),
            StatusCode::NOT_FOUND => Ok(()),
            // No `with_if_match` here: it maps SabreDAV's 412-on-missing to
            // NotFound, but delete reports an unmet precondition as
            // ConditionNotMatch.
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Delete")),
                resp,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use http::{Request, Response, StatusCode};

    use super::super::backend::WebdavBuilder;
    use super::*;

    struct FixedStatusTransport {
        status: StatusCode,
    }

    impl HttpTransport for FixedStatusTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            Ok(Response::builder()
                .status(self.status)
                .body(HttpBody::new(stream::empty(), Some(0)))
                .unwrap())
        }
    }

    fn operator_with_status(status: StatusCode) -> Result<Operator> {
        Ok(
            Operator::new(WebdavBuilder::default().endpoint("http://localhost:8080"))?
                .with_context(
                    OperationContext::new()
                        .with_http_transport(HttpTransporter::new(FixedStatusTransport { status })),
                ),
        )
    }

    #[tokio::test]
    async fn test_delete_accepts_200_202_204_and_404() -> Result<()> {
        for status in [
            StatusCode::OK,
            StatusCode::ACCEPTED,
            StatusCode::NO_CONTENT,
            StatusCode::NOT_FOUND,
        ] {
            let op = operator_with_status(status)?;
            op.delete("file.txt").await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_maps_unauthorized_and_conditional_not_found() -> Result<()> {
        let err = operator_with_status(StatusCode::UNAUTHORIZED)?
            .delete("file.txt")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(!err.is_temporary());

        let err = operator_with_status(StatusCode::NOT_FOUND)?
            .delete_with("file.txt")
            .if_match("\"etag\"")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        Ok(())
    }
}
