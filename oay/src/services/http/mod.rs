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

use actix_web::http::{Method, StatusCode};
use actix_web::web::Data;
use actix_web::{delete, middleware, web, HttpRequest, HttpResponse};
use actix_web::{App, HttpServer, Responder};
use log::debug;
use opendal::Operator;
use std::io;

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

    pub async fn start(self) -> io::Result<()> {
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

    fn get(&self, req: HttpRequest) -> HttpResponse {
        HttpResponse::new(StatusCode::OK)
    }
    fn put(&self, req: HttpRequest) -> HttpResponse {
        HttpResponse::new(StatusCode::CREATED)
    }
    fn head(&self, req: HttpRequest) -> HttpResponse {
        HttpResponse::new(StatusCode::OK)
    }
    fn delete(&self, req: HttpRequest) -> HttpResponse {
        HttpResponse::new(StatusCode::NO_CONTENT)
    }
}

async fn index(service: Data<Service>, req: HttpRequest) -> HttpResponse {
    match req.method().clone() {
        Method::GET => service.get_ref().get(req),
        Method::HEAD => service.get_ref().head(req),
        Method::PUT => service.get_ref().put(req),
        Method::DELETE => service.get_ref().delete(req),
        _ => HttpResponse::new(StatusCode::INTERNAL_SERVER_ERROR),
    }
}
