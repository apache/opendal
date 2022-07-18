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

use std::convert::Infallible;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use actix_web::body::SizedStream;
use actix_web::http;
use actix_web::http::header;
use actix_web::http::Method;
use actix_web::http::StatusCode;
use actix_web::middleware;
use actix_web::web;
use actix_web::web::Data;
use actix_web::App;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use anyhow::anyhow;
use futures::stream;
use futures::AsyncWriteExt;
use futures::StreamExt;
use log::error;
use log::warn;
use opendal::io_util::into_stream;
use opendal::Operator;

#[derive(Debug, Clone)]
pub struct Service {
    addr: String,
    op: Operator,
}

impl Service {
    pub fn new(addr: &str, op: Operator) -> Service {
        Service {
            addr: addr.to_string(),
            op,
        }
    }

    pub async fn start(self) -> Result<()> {
        let addr = self.addr.clone();

        HttpServer::new(move || {
            App::new()
                .app_data(Data::new(self.clone()))
                .wrap(middleware::Logger::default())
                .service(web::resource(r"{path:.*}").to(index))
        })
        .bind(&addr)?
        .run()
        .await
    }

    async fn get(&self, req: HttpRequest) -> Result<HttpResponse> {
        let o = self.op.object(req.path());

        let meta = o.metadata().await?;

        let r = if let Some(_range) = req.headers().get(header::RANGE) {
            todo!("implement range support")
        } else {
            o.reader().await?
        };

        Ok(HttpResponse::Ok().body(SizedStream::new(
            meta.content_length(),
            into_stream(r, 8 * 1024),
        )))
    }

    async fn put(&self, req: HttpRequest, body: web::Payload) -> Result<HttpResponse> {
        let o = self.op.object(req.path());

        let content_length: u64 = req
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("content-length is required"),
                )
            })?
            .to_str()
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("content-length is invalid: {e:?}"),
                )
            })?
            .parse()
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("content-length is invalid: {e:?}"),
                )
            })?;

        if content_length == 0 {
            o.create().await?
        } else {
            let mut w = o.writer(content_length).await?;
            let mut r = body;

            let mut n = 0u64;
            while let Some(bs) = r.next().await {
                let bs = bs.map_err(|e| {
                    Error::new(ErrorKind::UnexpectedEof, anyhow!("read body: {e:?}"))
                })?;
                w.write_all(&bs).await?;
                n += bs.len() as u64;
            }

            w.close().await?;

            if content_length != n {
                return Err(Error::new(ErrorKind::UnexpectedEof, anyhow!("short read")));
            }
        }

        Ok(HttpResponse::new(StatusCode::CREATED))
    }

    /// Actix handles content length automatically, we use a sized empty stream here to indicate:
    /// - The body's size.
    /// - The body returned by HEAD should not be read.
    async fn head(&self, req: HttpRequest) -> Result<HttpResponse> {
        let meta = self.op.object(req.path()).metadata().await?;

        Ok(HttpResponse::Ok().body(SizedStream::new(
            meta.content_length(),
            stream::empty::<std::result::Result<_, Infallible>>(),
        )))
    }

    async fn delete(&self, req: HttpRequest) -> Result<HttpResponse> {
        self.op.object(req.path()).delete().await?;
        Ok(HttpResponse::new(StatusCode::NO_CONTENT))
    }
}

async fn index(service: Data<Service>, req: HttpRequest, body: web::Payload) -> HttpResponse {
    let resp = match req.method().clone() {
        Method::GET => service.get_ref().get(req).await,
        Method::PUT => service.get_ref().put(req, body).await,
        Method::HEAD => service.get_ref().head(req).await,
        Method::DELETE => service.get_ref().delete(req).await,
        _ => Ok(HttpResponse::new(StatusCode::INTERNAL_SERVER_ERROR)),
    };

    match resp {
        Ok(resp) => match resp.error() {
            None => resp,
            Some(err) => {
                error!("request can't handle: {err:?}");
                HttpResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
        Err(err) if err.kind() == ErrorKind::NotFound => {
            warn!("resource not found: {err:?}");
            HttpResponse::new(StatusCode::NOT_FOUND)
        }
        Err(err) if err.kind() == ErrorKind::PermissionDenied => {
            warn!("resource permission denied: {err:?}");
            HttpResponse::new(StatusCode::FORBIDDEN)
        }
        Err(err) => {
            error!("request can't handle: {err:?}");
            HttpResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
