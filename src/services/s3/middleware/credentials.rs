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

use aws_smithy_http::middleware::AsyncMapRequest;
use aws_smithy_http::operation::Request;

use aws_types::credentials::{ProvideCredentials, SharedCredentialsProvider};
use std::future::Future;
use std::pin::Pin;

#[derive(Clone, Debug, Default)]
pub struct CredentialsStage;

impl CredentialsStage {
    /// Creates a new credentials stage.
    pub fn new() -> Self {
        CredentialsStage
    }

    async fn load_creds(mut request: Request) -> Result<Request, String> {
        let provider = request
            .properties()
            .get::<SharedCredentialsProvider>()
            .cloned();
        let provider = match provider {
            Some(provider) => provider,
            None => {
                return Ok(request);
            }
        };

        // We will ignore all credential loading errors here.
        if let Ok(creds) = provider.provide_credentials().await {
            request.properties_mut().insert(creds);
        }
        Ok(request)
    }
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl AsyncMapRequest for CredentialsStage {
    type Error = String;
    type Future = Pin<Box<dyn Future<Output = Result<Request, Self::Error>> + Send + 'static>>;

    fn apply(&self, request: Request) -> BoxFuture<Result<Request, Self::Error>> {
        Box::pin(Self::load_creds(request))
    }
}
