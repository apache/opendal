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

use std::time::Duration;

use futures::future::BoxFuture;
use http::Request;
use isahc::config::Configurable;

pub type HttpResponseFuture =
    BoxFuture<'static, Result<isahc::Response<isahc::AsyncBody>, isahc::Error>>;

/// HttpClient that used across opendal.
///
/// NOTE: we could change or support more underlying http backend.
#[derive(Debug, Clone)]
pub struct HttpClient(isahc::HttpClient);

impl HttpClient {
    /// Create a new http client.
    ///
    /// # TODO
    ///
    /// We will allow users to config the max connections and timeout.
    ///
    /// But we don't know the correct API that exposed to end users.
    /// Let's pick up reasonable settings here until we have a great solutions.
    pub fn new() -> Self {
        let client = isahc::HttpClient::builder()
            // As discussed in aws docs: too large connections is helpless for the total throughout.
            //
            // The most high-end of AWS EC2's network performance is 100 Gbps. 128 connections is able
            // to full use the entire bandwidth at nearly 100 MB/s per-conn.
            //
            // ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html#optimizing-performance-parallelization
            .max_connections(128)
            // The default connect timeout is 300 seconds that is too long for our cases.
            // We expect OpenDAL is running on a more stable networking.
            // Thus, we decrease the value to 10s.
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("client init must succeed");

        HttpClient(client)
    }

    /// Send a request in blocking way.
    pub fn send(
        &self,
        req: Request<isahc::Body>,
    ) -> Result<isahc::Response<isahc::Body>, isahc::Error> {
        self.0.send(req)
    }

    /// Send a request in async way.
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
