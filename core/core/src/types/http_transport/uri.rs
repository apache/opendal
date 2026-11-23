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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use percent_encoding::percent_decode_str;

/// An HTTP URI or relative location with a precomputed diagnostic representation.
///
/// Construction preserves the original spelling and masks user information and
/// recognized credential query values in a separate representation. It does not
/// validate, resolve, or normalize the URI. Ordinary query values remain intact.
/// Clones share both representations; formatting does not repeat redaction.
///
/// Recognized query names include OAuth credentials, storage session tokens,
/// and signed URL credential identifiers and signatures such as
/// `X-Amz-Credential`, `X-Goog-Signature`, and `sig`. Names are matched
/// case-insensitively after percent-decoding. Arbitrary application parameters
/// are not classified.
#[derive(Clone)]
pub struct HttpUri {
    original: Arc<str>,
    redacted: Arc<str>,
}

impl HttpUri {
    /// Retain the original URI and compute its diagnostic representation once.
    pub fn new(uri: impl Into<Arc<str>>) -> Self {
        let original = uri.into();
        let redacted = redact_uri(&original).into();
        Self { original, redacted }
    }

    /// Return the original URI, including any credentials it contains.
    pub fn original_uri(&self) -> &str {
        &self.original
    }

    /// Return the URI with recognized credentials masked for diagnostics.
    pub fn redacted_uri(&self) -> &str {
        &self.redacted
    }

    /// Get or create the diagnostic value for a response's `Location` header.
    ///
    /// Store it in response extensions so service errors and shared response
    /// diagnostics reuse the same value. If a wrapper changes the header,
    /// replace the cached value. Return `None` for missing or non-text headers.
    /// The original header is not changed.
    pub fn from_response_location(parts: &mut http::response::Parts) -> Option<&Self> {
        let location = parts.headers.get(http::header::LOCATION)?.to_str().ok()?;
        if parts
            .extensions
            .get::<Self>()
            .is_none_or(|uri| uri.original_uri() != location)
        {
            let uri = Self::new(location);
            parts.extensions.insert(uri);
        }
        parts.extensions.get::<Self>()
    }
}

impl Debug for HttpUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("HttpUri")
            .field(&self.redacted_uri())
            .finish()
    }
}

/// A reusable HTTP redirect handled by the transport.
///
/// A transport that supports this extension may return it with a response after
/// following redirects. Services can copy it into later request extensions to
/// reuse that destination while keeping the original request URI for diagnostics.
/// The transport must send to the destination and apply its redirect credential
/// policy, including credentials supplied by client defaults.
///
/// The destination retains its original and redacted representations in
/// [`HttpUri`]. Only the transport should use the original as a request URI.
/// Services may inspect it to determine whether reuse is valid, but must use
/// the redacted representation for diagnostics. This extension alone does not
/// establish that the original path still selects the same object. Transports
/// that do not support reusable redirects must not return it.
#[derive(Clone)]
pub struct HttpRedirect(HttpUri);

impl HttpRedirect {
    /// Record a destination that this transport can reuse.
    pub fn new(uri: http::Uri) -> Self {
        Self(HttpUri::new(uri.to_string()))
    }

    /// Return the destination's original and diagnostic representations.
    pub fn uri(&self) -> &HttpUri {
        &self.0
    }
}

impl Debug for HttpRedirect {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("HttpRedirect")
            .field(&self.0.redacted_uri())
            .finish()
    }
}

fn redact_uri(value: &str) -> String {
    let mut value = value.to_string();
    let authority_start = value
        .split_once("://")
        .filter(|(scheme, _)| {
            scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
        })
        .map(|(scheme, _)| scheme.len() + 3)
        .or_else(|| value.starts_with("//").then_some(2));
    if let Some(start) = authority_start {
        let end = value[start..]
            .find(['/', '?', '#'])
            .map_or(value.len(), |offset| start + offset);
        if let Some(offset) = value[start..end].rfind('@') {
            value.replace_range(start..start + offset, "[REDACTED]");
        }
    }

    let end = value.find('#').unwrap_or(value.len());
    if let Some(start) = value[..end].find('?') {
        let query = value[start + 1..end]
            .split('&')
            .map(|pair| {
                let Some((key, _)) = pair.split_once('=') else {
                    return pair.to_string();
                };
                let name = percent_decode_str(key).decode_utf8_lossy();
                match name.to_ascii_lowercase().as_str() {
                    "access_token"
                    | "accesskeyid"
                    | "authorization"
                    | "client_secret"
                    | "delegation"
                    | "oidctoken"
                    | "ossaccesskeyid"
                    | "password"
                    | "policy"
                    | "q-ak"
                    | "q-signature"
                    | "refresh_token"
                    | "security-token"
                    | "securitytoken"
                    | "sig"
                    | "signature"
                    | "skoid"
                    | "sktid"
                    | "temp_url_sig"
                    | "token"
                    | "upload_id"
                    | "x-amz-s3session-token"
                    | "x-amz-credential"
                    | "x-amz-security-token"
                    | "x-amz-signature"
                    | "x-cos-security-token"
                    | "x-goog-credential"
                    | "x-goog-signature"
                    | "x-oss-access-key-id"
                    | "x-oss-credential"
                    | "x-oss-security-token"
                    | "x-oss-signature"
                    | "x-tos-credential"
                    | "x-tos-security-token"
                    | "x-tos-signature" => format!("{key}=[REDACTED]"),
                    _ => pair.to_string(),
                }
            })
            .collect::<Vec<_>>()
            .join("&");
        value.replace_range(start + 1..end, &query);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_credential_query_values() {
        for key in [
            "access_token",
            "client_secret",
            "delegation",
            "Policy",
            "q-signature",
            "refresh_token",
            "security-token",
            "Si%67",
            "Signature",
            "token",
            "X-Amz-S3session-Token",
            "X-Amz-Security-Token",
            "X-Amz-Signature",
            "X-Cos-Security-Token",
            "X-Goog-Signature",
            "x-oss-security-token",
            "x-oss-signature",
            "X-Tos-Security-Token",
            "X-Tos-Signature",
        ] {
            let metadata =
                "sv=2024-08-04&X-Amz-Expires=60&x-oss-signature-version=OSS4-HMAC-SHA256&flag";
            let original =
                format!("https://example.com/object?{metadata}&{key}=test-value&{key}=other-value");
            let uri = HttpUri::new(original.clone());
            assert_eq!(uri.original_uri(), original);
            assert_eq!(
                uri.redacted_uri(),
                format!("https://example.com/object?{metadata}&{key}=[REDACTED]&{key}=[REDACTED]")
            );
            assert!(!format!("{uri:?}").contains("-value"));
        }
    }

    #[test]
    fn redacts_signing_and_authentication_query_values() {
        for key in [
            "AccessKeyId",
            "Authorization",
            "OIDCToken",
            "OSSAccessKeyId",
            "password",
            "q-ak",
            "SecurityToken",
            "skoid",
            "sktid",
            "upload_id",
            "X-Amz-Credential",
            "X-Goog-%43redential",
            "x-oss-access-key-id",
            "x-oss-credential",
            "X-Tos-Credential",
        ] {
            let original = format!("https://example.com/object?{key}=fixture%2Fvalue&version=3");
            let uri = HttpUri::new(original.clone());
            assert_eq!(uri.original_uri(), original);
            assert_eq!(
                uri.redacted_uri(),
                format!("https://example.com/object?{key}=[REDACTED]&version=3")
            );
            assert!(!format!("{uri:?}").contains("fixture%2Fvalue"));
        }
    }

    #[test]
    fn redacts_swift_temp_url_signature_and_preserves_scope() {
        let original = "/container?temp_url_sig=sha256%3Afixture&temp_url_expires=1234567890&temp_url_prefix=images%2F";
        let uri = HttpUri::new(original);
        assert_eq!(uri.original_uri(), original);
        assert_eq!(
            uri.redacted_uri(),
            "/container?temp_url_sig=[REDACTED]&temp_url_expires=1234567890&temp_url_prefix=images%2F"
        );
        assert!(!format!("{uri:?}").contains("sha256%3Afixture"));
    }
}
