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

use std::ops::Deref;

/// HttpClient that used across opendal.
///
/// NOTE: we could change or support more underlying http backend.
#[derive(Debug, Clone)]
pub struct HttpClient(
    hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>,
);

impl HttpClient {
    /// Create a new http client.
    pub fn new() -> Self {
        HttpClient(hyper::Client::builder().build(hyper_tls::HttpsConnector::new()))
    }
}

/// Forward all function to http backend.
impl Deref for HttpClient {
    type Target =
        hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
