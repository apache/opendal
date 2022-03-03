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

use std::fmt::Debug;

use aws_endpoint::AwsEndpointStage;
use aws_http::auth::CredentialsStage;
use aws_http::recursion_detection::RecursionDetectionStage;
use aws_http::user_agent::UserAgentStage;
use aws_sig_auth::signer::SigV4Signer;
use aws_smithy_client::retry::Config as RetryConfig;
use aws_smithy_http_tower::map_request::AsyncMapRequestLayer;
use aws_smithy_http_tower::map_request::MapRequestLayer;
use tower::layer::util::Identity;
use tower::layer::util::Stack;
use tower::ServiceBuilder;

use super::signer::SigningStage;

type DefaultMiddlewareStack = Stack<
    MapRequestLayer<RecursionDetectionStage>,
    Stack<
        MapRequestLayer<SigningStage>,
        Stack<
            AsyncMapRequestLayer<CredentialsStage>,
            Stack<
                MapRequestLayer<UserAgentStage>,
                Stack<MapRequestLayer<AwsEndpointStage>, Identity>,
            >,
        >,
    >,
>;

/// AWS Middleware Stack
///
/// This implements the middleware stack for this service. It will:
/// 1. Load credentials asynchronously into the property bag
/// 2. Sign the request with SigV4
/// 3. Resolve an Endpoint for the request
/// 4. Add a user agent to the request
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct DefaultMiddleware;

impl DefaultMiddleware {
    pub fn new() -> Self {
        Self {}
    }
}

// define the middleware stack in a non-generic location to reduce code bloat.
fn base() -> ServiceBuilder<DefaultMiddlewareStack> {
    let credential_provider = AsyncMapRequestLayer::for_mapper(CredentialsStage::new());
    let signer = MapRequestLayer::for_mapper(SigningStage::new(SigV4Signer::new()));
    let endpoint_resolver = MapRequestLayer::for_mapper(AwsEndpointStage);
    let user_agent = MapRequestLayer::for_mapper(UserAgentStage::new());
    let recursion_detection = MapRequestLayer::for_mapper(RecursionDetectionStage::new());
    // These layers can be considered as occurring in order, that is:
    // 1. Resolve an endpoint
    // 2. Add a user agent
    // 3. Acquire credentials
    // 4. Sign with credentials
    // (5. Dispatch over the wire)
    ServiceBuilder::new()
        .layer(endpoint_resolver)
        .layer(user_agent)
        .layer(credential_provider)
        .layer(signer)
        .layer(recursion_detection)
}

impl<S> tower::Layer<S> for DefaultMiddleware {
    type Service = <DefaultMiddlewareStack as tower::Layer<S>>::Service;

    fn layer(&self, inner: S) -> Self::Service {
        base().service(inner)
    }
}
