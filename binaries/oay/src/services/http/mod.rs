// Copyright 2022 Datafuse Labs
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
use std::str::FromStr;

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
use futures::try_join;
use futures::AsyncWriteExt;
use futures::StreamExt;
use log::error;
use log::warn;
use opendal::raw::input::into_stream;
use opendal::raw::BytesContentRange;
use opendal::raw::BytesRange;
use opendal::Operator;
use percent_encoding::percent_decode;

use crate::env;

#[derive(Debug, Clone)]
pub struct Service {
    addr: String,
    op: Operator,
}

impl Service {
    pub async fn new() -> Result<Service> {
        Ok(Service {
            addr: env::get_oay_addr(),
            op: env::get_oay_operator().await?,
        })
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
        let o = self
            .op
            .object(&percent_decode(req.path().as_bytes()).decode_utf8_lossy());

        let meta = o.stat().await?;

        let (size, r) = if let Some(range) = req.headers().get(header::RANGE) {
            let br = BytesRange::from_str(range.to_str().map_err(|e| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("header range is invalid: {e}"),
                )
            })?)?;

            let bcr = BytesContentRange::from_bytes_range(meta.content_length(), br);

            (
                bcr.len().expect("range must be specified"),
                o.range_reader(bcr.range().expect("range must be specified"))
                    .await?,
            )
        } else {
            (meta.content_length(), o.reader().await?)
        };

        Ok(HttpResponse::Ok().body(SizedStream::new(size, into_stream(r, 8 * 1024))))
    }

    async fn put(&self, req: HttpRequest, mut body: web::Payload) -> Result<HttpResponse> {
        let o = self
            .op
            .object(&percent_decode(req.path().as_bytes()).decode_utf8_lossy());

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
            let (pr, mut pw) = sluice::pipe::pipe();

            try_join!(
                async {
                    o.write_from(content_length, pr).await?;

                    Ok::<(), Error>(())
                },
                async {
                    while let Some(bs) = body.next().await {
                        let bs = bs.map_err(|e| {
                            Error::new(ErrorKind::UnexpectedEof, anyhow!("read body: {e:?}"))
                        })?;
                        pw.write_all(&bs).await?;
                    }

                    pw.close().await?;

                    Ok::<(), Error>(())
                }
            )?;
        }

        Ok(HttpResponse::new(StatusCode::CREATED))
    }

    /// Actix handles content length automatically, we use a sized empty stream here to indicate:
    /// - The body's size.
    /// - The body returned by HEAD should not be read.
    async fn head(&self, req: HttpRequest) -> Result<HttpResponse> {
        let o = self
            .op
            .object(&percent_decode(req.path().as_bytes()).decode_utf8_lossy());
        let meta = o.stat().await?;

        Ok(HttpResponse::Ok().body(SizedStream::new(
            meta.content_length(),
            stream::empty::<std::result::Result<_, Infallible>>(),
        )))
    }

    async fn delete(&self, req: HttpRequest) -> Result<HttpResponse> {
        self.op
            .object(&percent_decode(req.path().as_bytes()).decode_utf8_lossy())
            .delete()
            .await?;
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
