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

/// We borrowed code from `aws_sig_auth` here to make anonymous access possible.
///
/// The original implementations requires `Credentials` and signing all requests.
/// We did a simple trick here: rewrite the `SigningStage` and only sign request
/// when we have a valid credentials.
///
/// For users who specify Credentials, nothing changed.
/// For users who doesn't specify Credentials, there are two situations:
///
/// - The env could have valid credentials, we will load credentials from env.
/// - There aren't any credentials, we will sending request without any signing
///   just like sending requests via browser or `curl`.
///
/// # TODO
///
/// There is a potential CVE. Users could construct an anonymous client to read
/// credentials from the environment. We should address it in the future.
use std::time::SystemTime;

use aws_sig_auth::middleware::SigningStageError;
use aws_sig_auth::signer::OperationSigningConfig;
use aws_sig_auth::signer::RequestConfig;
use aws_sig_auth::signer::SigV4Signer;
use aws_sig_auth::signer::SigningRequirements;
use aws_sigv4::http_request::SignableBody;
use aws_smithy_http::middleware::MapRequest;
use aws_smithy_http::operation::Request;
use aws_smithy_http::property_bag::PropertyBag;
use aws_types::region::SigningRegion;
use aws_types::Credentials;
use aws_types::SigningService;

#[derive(Clone, Debug)]
pub struct SigningStage {
    signer: SigV4Signer,
}

impl SigningStage {
    pub fn new(signer: SigV4Signer) -> Self {
        Self { signer }
    }
}

fn signing_config(
    config: &PropertyBag,
) -> Result<(&OperationSigningConfig, RequestConfig, Option<Credentials>), SigningStageError> {
    let operation_config = config
        .get::<OperationSigningConfig>()
        .ok_or(SigningStageError::MissingSigningConfig)?;
    // Here is a trick.
    // We will return `Option<Credentials>` here instead of `Credentials`.
    let credentials = config.get::<Credentials>().cloned();
    let region = config
        .get::<SigningRegion>()
        .ok_or(SigningStageError::MissingSigningRegion)?;
    let signing_service = config
        .get::<SigningService>()
        .ok_or(SigningStageError::MissingSigningService)?;
    let payload_override = config.get::<SignableBody<'static>>();
    let request_config = RequestConfig {
        request_ts: config
            .get::<SystemTime>()
            .copied()
            .unwrap_or_else(SystemTime::now),
        region,
        payload_override,
        service: signing_service,
    };
    Ok((operation_config, request_config, credentials))
}

impl MapRequest for SigningStage {
    type Error = SigningStageError;

    fn apply(&self, req: Request) -> Result<Request, Self::Error> {
        req.augment(|mut req, config| {
            let operation_config = config
                .get::<OperationSigningConfig>()
                .ok_or(SigningStageError::MissingSigningConfig)?;
            let (operation_config, request_config, creds) =
                match &operation_config.signing_requirements {
                    SigningRequirements::Disabled => return Ok(req),
                    SigningRequirements::Optional => match signing_config(config) {
                        Ok(parts) => parts,
                        Err(_) => return Ok(req),
                    },
                    SigningRequirements::Required => signing_config(config)?,
                };

            // The most tricky part here.
            //
            // We will try to load the credentials and only sign it when we have a
            // valid credential.
            if let Some(creds) = creds {
                let signature = self
                    .signer
                    .sign(operation_config, &request_config, &creds, &mut req)
                    .map_err(|err| SigningStageError::SigningFailure(err))?;
                config.insert(signature);
            }

            Ok(req)
        })
    }
}
