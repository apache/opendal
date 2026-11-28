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

use http::Uri;
use http::response::Parts;

use crate::Error;
use crate::ErrorKind;
use crate::HttpUri;

/// Create a new error happened during building request.
pub fn new_request_build_error(err: http::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "building http request")
        .with_operation("http::Request::build")
        .set_source(err)
}

/// Create a new error happened during signing request.
pub fn new_request_credential_error(err: anyhow::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "loading credential to sign http request",
    )
    .set_temporary()
    .with_operation("reqsign::LoadCredential")
    .set_source(err)
}

/// Wrap a signing failure, preserving reqsign's retryability when available.
///
/// A failure caused by a missing reqsign `CryptoProvider` returns
/// [`ErrorKind::ConfigInvalid`], as described by
/// [`new_crypto_provider_missing_error`].
pub fn new_request_sign_error(err: anyhow::Error) -> Error {
    #[cfg(feature = "reqsign")]
    if let Some(error) = err
        .downcast_ref::<reqsign_core::Error>()
        .and_then(new_crypto_provider_missing_error)
    {
        return error.with_operation("reqsign::Sign").set_source(err);
    }

    let error = Error::new(ErrorKind::Unexpected, "signing http request");
    #[cfg(feature = "reqsign")]
    let error = error.with_temporary(
        err.downcast_ref::<reqsign_core::Error>()
            .is_some_and(reqsign_core::Error::is_retryable),
    );
    error.with_operation("reqsign::Sign").set_source(err)
}

/// Return a [`ErrorKind::ConfigInvalid`] error when a reqsign failure is caused
/// by a missing `CryptoProvider`.
///
/// This function returns `Some` only when the `reqsign-rustcrypto` feature is
/// disabled, no process-wide provider is installed, and `err` is the error
/// reqsign returns from `Context::crypto_provider` in that state. The error
/// message names both remedies. Every other error, including other reqsign
/// `ConfigInvalid` failures, returns `None`.
#[cfg(feature = "reqsign")]
pub fn new_crypto_provider_missing_error(err: &reqsign_core::Error) -> Option<Error> {
    if cfg!(feature = "reqsign-rustcrypto") || err.kind() != reqsign_core::ErrorKind::ConfigInvalid
    {
        return None;
    }
    let missing = reqsign_core::Context::new().crypto_provider().err()?;
    if err.to_string() != missing.to_string() {
        return None;
    }

    Some(Error::new(
        ErrorKind::ConfigInvalid,
        "no reqsign CryptoProvider is available to sign requests; enable the `reqsign-rustcrypto` \
         feature, or call `opendal::reqsign_core::crypto::install_default` before building the \
         operator",
    ))
}

/// Add response context to error.
///
/// This helper function will:
///
/// - remove sensitive or useless headers from parts.
/// - include a redacted URI if the response extensions contain `Uri`.
pub fn with_error_response_context(mut err: Error, mut parts: Parts) -> Error {
    if let Some(uri) = parts.extensions.get::<Uri>() {
        err = err.with_context("uri", HttpUri::new(uri.to_string()).redacted_uri());
    }

    // The following headers may contains sensitive information.
    parts.headers.remove("Set-Cookie");
    parts.headers.remove("WWW-Authenticate");
    parts.headers.remove("Proxy-Authenticate");

    if parts.headers.contains_key(http::header::LOCATION) {
        let location = HttpUri::from_response_location(&mut parts)
            .map(|uri| uri.redacted_uri())
            .unwrap_or("<invalid URL>")
            .parse()
            .unwrap_or_else(|_| http::HeaderValue::from_static("<invalid URL>"));
        parts.headers.insert(http::header::LOCATION, location);
    }

    err = err.with_context("response", format!("{parts:?}"));

    err
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_request_and_location_in_response_error() {
        let request_uri: Uri = concat!(
            "https://example.com/object?access_token=",
            "request-secret",
            "&version=3"
        )
        .parse()
        .unwrap();
        let (parts, ()) = http::Response::builder()
            .status(403)
            .extension(request_uri)
            .header(
                http::header::LOCATION,
                concat!("/next?access_token=", "redirect-secret", "&version=4"),
            )
            .body(())
            .unwrap()
            .into_parts();
        let error =
            with_error_response_context(Error::new(ErrorKind::PermissionDenied, "denied"), parts);

        for diagnostic in [
            error.to_string(),
            format!("{error:?}"),
            format!("{error:#?}"),
        ] {
            assert!(
                diagnostic.contains("https://example.com/object?access_token=[REDACTED]&version=3")
            );
            assert!(diagnostic.contains("/next?access_token=[REDACTED]&version=4"));
            assert!(!diagnostic.contains("request-secret"));
            assert!(!diagnostic.contains("redirect-secret"));
        }
        assert!(
            error
                .to_string()
                .contains("uri: https://example.com/object?access_token=[REDACTED]&version=3")
        );
    }
}
