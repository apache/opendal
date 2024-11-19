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

use std::convert::Infallible;
use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use axum::routing::any_service;
use axum::Router;
use dav_server::DavHandler;
use dav_server_opendalfs::OpendalFs;
use opendal::Operator;

use crate::Config;

pub struct WebdavService {
    cfg: Arc<Config>,
    opendalfs: Box<OpendalFs>,
}

impl WebdavService {
    pub fn new(cfg: Arc<Config>, op: Operator) -> Self {
        Self {
            cfg,
            opendalfs: OpendalFs::new(op),
        }
    }

    pub async fn serve(&self) -> anyhow::Result<()> {
        let webdav_cfg = &self.cfg.frontends.webdav;

        let webdav_handler = DavHandler::builder()
            .filesystem(self.opendalfs.clone())
            .build_handler();

        let webdav_service = tower::service_fn(move |req: Request<Body>| {
            let webdav_server = webdav_handler.clone();
            async move { Ok::<_, Infallible>(webdav_server.handle(req).await) }
        });

        let app = Router::new().route("/*path", any_service(webdav_service));

        let listener = tokio::net::TcpListener::bind(&webdav_cfg.addr)
            .await
            .unwrap();
        axum::serve(listener, app.into_make_service()).await?;

        Ok(())
    }
}
