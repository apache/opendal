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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::anyhow;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use chrono::SecondsFormat;
use opendal::Lister;
use opendal::Metakey;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::debug;
use uuid::Uuid;

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
            .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http()))
            .with_state(S3State {
                op: self.op.clone(),
                list_objects: Arc::default(),
            });

        axum::Server::bind(&s3_cfg.addr.parse().unwrap())
            .serve(app.into_make_service())
            .await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct S3State {
    op: Operator,
    /// TODO: remove this global lock by page checkpoint.
    list_objects: Arc<Mutex<HashMap<String, Lister>>>,
}

/// # TODO
///
/// we need to support following parameters:
///
/// - max-keys
/// - start-after
#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct ListObjectsV2Params {
    prefix: String,
    delimiter: String,
    continuation_token: String,
}

async fn handle_list_objects(
    state: State<S3State>,
    params: Query<ListObjectsV2Params>,
) -> Result<OkResponse, ErrorResponse> {
    debug!("got params: {:?}", params);
    if params.delimiter != "/" && !params.delimiter.is_empty() {
        return Err(anyhow!("delimiter is not supported").into());
    }

    let lister = state
        .list_objects
        .lock()
        .unwrap()
        .remove(format!("{}-{}", params.prefix, params.continuation_token).as_str());

    let mut lister = match lister {
        Some(lister) => lister,
        None => {
            if params.delimiter.is_empty() {
                state.op.list(&params.prefix).await?
            } else {
                state.op.scan(&params.prefix).await?
            }
        }
    };

    let page = lister.next_page().await?.unwrap_or_default();

    let is_truncated = lister.has_next().await?;

    let (mut common_prefixes, mut contents) = (vec![], vec![]);
    for v in page {
        let meta = state
            .op
            .metadata(
                &v,
                Metakey::Mode | Metakey::LastModified | Metakey::Etag | Metakey::ContentLength,
            )
            .await?;

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

    let next_continuation_token = if is_truncated {
        let token = format!("{}-{}", params.prefix, Uuid::new_v4().as_u128());
        // Insert the lister into the state so that we can continue listing
        state
            .list_objects
            .lock()
            .unwrap()
            .insert(token.clone(), lister);
        token
    } else {
        String::new()
    };

    let resp = ListBucketResult {
        is_truncated,
        common_prefixes,
        contents,
        continuation_token: params.continuation_token.to_string(),
        next_continuation_token,
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
    continuation_token: String,
    next_continuation_token: String,
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
