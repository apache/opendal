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

use futures::future::BoxFuture;
use http::Request;

pub type HttpResponseFuture =
    BoxFuture<'static, Result<isahc::Response<isahc::AsyncBody>, isahc::Error>>;

/// HttpClient that used across opendal.
///
/// NOTE: we could change or support more underlying http backend.
#[derive(Debug, Clone)]
pub struct HttpClient(isahc::HttpClient);

impl HttpClient {
    /// Create a new http client.
    pub fn new() -> Self {
        HttpClient(
            isahc::HttpClient::builder()
                .max_connections(128)
                .max_connections_per_host(64)
                .build()
                .expect("client init must succeed"),
        )
    }

    pub fn send(
        &self,
        req: Request<isahc::Body>,
    ) -> Result<isahc::Response<isahc::Body>, isahc::Error> {
        self.0.send(req)
    }

    pub fn send_async(&self, req: Request<isahc::AsyncBody>) -> HttpResponseFuture {
        let client = self.0.clone();

        Box::pin(async move { client.send_async(req).await })
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::new()
    }
}
