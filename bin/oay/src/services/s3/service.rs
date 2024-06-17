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

use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use chrono::SecondsFormat;
use futures_util::StreamExt;
use opendal::Metakey;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;
use tracing::debug;

use crate::Config;

pub struct S3Service {
    cfg: Arc<Config>,
    op: Operator,
}

impl S3Service {
    pub fn new(cfg: Arc<Config>, op: Operator) -> Self {
        Self { cfg, op }
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let s3_cfg = &self.cfg.frontends.s3;

        let app = Router::new()
            .route("/", get(handle_list_objects))
            .with_state(S3State {
                op: self.op.clone(),
            });


        let listener = tokio::net::TcpListener::bind(&s3_cfg.addr)
            .await
            .unwrap();
        axum::serve(listener, app.into_make_service()).await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct S3State {
    op: Operator,
}

/// # TODO
///
/// we need to support following parameters:
///
/// - max-keys
/// - continuation_token
#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct ListObjectsV2Params {
    prefix: String,
    start_after: String,
}

async fn handle_list_objects(
    state: State<S3State>,
    params: Query<ListObjectsV2Params>,
) -> Result<OkResponse, ErrorResponse> {
    debug!("got params: {:?}", params);

    if !state.op.info().full_capability().list_with_start_after {
        return Err(ErrorResponse {
            code: StatusCode::NOT_IMPLEMENTED,
            err: Error {
                code: "NotImplemented".to_string(),
                message: "list with start after is not supported".to_string(),
                resource: "".to_string(),
                request_id: "".to_string(),
            },
        });
    }

    let mut lister = state
        .op
        .lister_with(&params.prefix)
        .start_after(&params.start_after)
        .metakey(Metakey::Mode | Metakey::LastModified | Metakey::Etag | Metakey::ContentLength)
        .await?
        .chunks(1000);

    let page = lister.next().await.unwrap_or_default();

    let is_truncated = page.len() >= 1000;

    let (mut common_prefixes, mut contents) = (vec![], vec![]);
    for v in page {
        let v = v?;
        let meta = v.metadata();

        if meta.is_dir() {
            common_prefixes.push(CommonPrefix {
                prefix: v.path().to_string(),
            });
        } else {
            contents.push(Object {
                key: v.path().to_string(),
                last_modified: meta
                    .last_modified()
                    .unwrap_or_default()
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
                etag: meta.etag().unwrap_or_default().to_string(),
                size: meta.content_length(),
            });
        }
    }

    let resp = ListBucketResult {
        is_truncated,
        common_prefixes,
        contents,
        start_after: Some(params.start_after.clone()),
    };

    Ok(OkResponse {
        code: StatusCode::OK,
        content: quick_xml::se::to_string(&resp).unwrap().into_bytes(),
    })
}

#[derive(Serialize, Default)]
#[serde(default, rename_all = "PascalCase")]
struct ListBucketResult {
    is_truncated: bool,
    common_prefixes: Vec<CommonPrefix>,
    contents: Vec<Object>,
    start_after: Option<String>,
}

#[derive(Serialize, Default)]
#[serde(default, rename_all = "PascalCase")]
struct CommonPrefix {
    prefix: String,
}

#[derive(Serialize, Default)]
#[serde(default, rename_all = "PascalCase")]
struct Object {
    key: String,
    last_modified: String,
    etag: String,
    size: u64,
}

struct OkResponse {
    code: StatusCode,
    content: Vec<u8>,
}

impl IntoResponse for OkResponse {
    fn into_response(self) -> Response {
        (self.code, self.content).into_response()
    }
}

struct ErrorResponse {
    code: StatusCode,
    err: Error,
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        (self.code, quick_xml::se::to_string(&self.err).unwrap()).into_response()
    }
}

#[derive(Serialize)]
#[serde(default, rename_all = "PascalCase")]
struct Error {
    code: String,
    message: String,
    resource: String,
    request_id: String,
}

impl From<opendal::Error> for ErrorResponse {
    fn from(err: opendal::Error) -> Self {
        let err = Error {
            code: "InternalError".to_string(),
            message: err.to_string(),
            resource: "".to_string(),
            request_id: "".to_string(),
        };

        ErrorResponse {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            err,
        }
    }
}

impl From<anyhow::Error> for ErrorResponse {
    fn from(err: anyhow::Error) -> Self {
        let err = Error {
            code: "InternalError".to_string(),
            message: err.to_string(),
            resource: "".to_string(),
            request_id: "".to_string(),
        };

        ErrorResponse {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            err,
        }
    }
}
