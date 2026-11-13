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

use crate::config::BaiduNetdiskConfig;
use asyncband::mutex::Mutex;
use bytes::Buf;
use http::{Request, header};
use opendal_core::{raw::*, *};
use serde::Deserialize;
use std::{fmt, sync::Arc, time::Duration};

pub type TokenRefreshCallback = Arc<dyn Fn(&str, &str) -> Result<()> + Send + Sync>;

pub struct BaiduNetdiskSigner {
    client_id: String,
    client_secret: String,
    state: Mutex<TokenState>,
    on_refresh: Option<TokenRefreshCallback>,
}

struct TokenState {
    access_token: String,
    refresh_token: Option<String>,
    expires_at: Option<Timestamp>,
    pending_persistence: bool,
}

impl fmt::Debug for BaiduNetdiskSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BaiduNetdiskSigner").finish_non_exhaustive()
    }
}

impl BaiduNetdiskSigner {
    pub fn new(
        config: &BaiduNetdiskConfig,
        on_refresh: Option<TokenRefreshCallback>,
    ) -> Result<Self> {
        let refresh = [
            &config.client_id,
            &config.client_secret,
            &config.refresh_token,
        ];
        let complete = refresh
            .iter()
            .all(|v| v.as_ref().is_some_and(|v| !v.is_empty()));
        if refresh.iter().any(|v| v.is_some()) && !complete {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "client_id, client_secret and refresh_token must be configured together",
            ));
        }
        if config.access_token.as_ref().is_none_or(|v| v.is_empty()) && !complete {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "access_token or refresh credentials are required",
            ));
        }
        Ok(Self {
            client_id: config.client_id.clone().unwrap_or_default(),
            client_secret: config.client_secret.clone().unwrap_or_default(),
            state: Mutex::new(TokenState {
                access_token: config.access_token.clone().unwrap_or_default(),
                refresh_token: config.refresh_token.clone(),
                expires_at: None,
                pending_persistence: false,
            }),
            on_refresh,
        })
    }

    pub async fn token(&self, ctx: &OperationContext) -> Result<String> {
        let mut state = self.state.lock().await;
        if state.access_token.is_empty()
            || state
                .expires_at
                .is_some_and(|expiry| expiry <= Timestamp::now() + Duration::from_secs(120))
        {
            // Baidu refresh tokens are single-use, including uncertain failures.
            // Taking it before I/O prevents concurrent callers or retries from replaying it.
            let refresh_token = state.refresh_token.take().ok_or_else(|| {
                Error::new(
                    ErrorKind::PermissionDenied,
                    "Baidu Netdisk requires a new authorization",
                )
            })?;
            state.access_token.clear();
            let query = form_urlencoded::Serializer::new(String::new())
                .append_pair("grant_type", "refresh_token")
                .append_pair("client_id", &self.client_id)
                .append_pair("client_secret", &self.client_secret)
                .append_pair("refresh_token", &refresh_token)
                .finish();
            let request =
                Request::get(format!("https://openapi.baidu.com/oauth/2.0/token?{query}"))
                    .header(header::USER_AGENT, "pan.baidu.com")
                    .body(Buffer::new())
                    .map_err(new_request_build_error)?;
            let started = Timestamp::now();
            let response = ctx.http_transport().send(request).await.map_err(|_| {
                Error::new(
                    ErrorKind::PermissionDenied,
                    "Baidu OAuth refresh failed; reauthorization required",
                )
            })?;
            if !response.status().is_success() {
                return Err(Error::new(
                    ErrorKind::PermissionDenied,
                    "Baidu OAuth refresh rejected; reauthorization required",
                ));
            }
            let token: TokenResponse = serde_json::from_reader(response.into_body().reader())
                .map_err(|_| {
                    Error::new(
                        ErrorKind::PermissionDenied,
                        "invalid Baidu OAuth token response; reauthorization required",
                    )
                })?;
            if token.access_token.is_empty()
                || token.refresh_token.is_empty()
                || token.expires_in == 0
            {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "incomplete Baidu OAuth token response",
                ));
            }
            let seconds = i64::try_from(token.expires_in)
                .ok()
                .and_then(|v| started.into_inner().as_second().checked_add(v))
                .ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "invalid Baidu OAuth expiration")
                })?;
            state.expires_at = Some(Timestamp::from_second(seconds).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "invalid Baidu OAuth expiration").set_source(e)
            })?);
            state.access_token = token.access_token;
            state.refresh_token = Some(token.refresh_token);
            state.pending_persistence = self.on_refresh.is_some();
        }
        if state.pending_persistence {
            if let Some(callback) = &self.on_refresh {
                callback(
                    &state.access_token,
                    state
                        .refresh_token
                        .as_deref()
                        .expect("successful refresh has a refresh token"),
                )?;
            }
            state.pending_persistence = false;
        }
        Ok(state.access_token.clone())
    }

    pub async fn invalidate(&self, rejected_token: &str) -> bool {
        let mut state = self.state.lock().await;
        if state.access_token != rejected_token {
            return true;
        }
        if state.refresh_token.is_none() {
            return false;
        }
        state.access_token.clear();
        true
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
}

pub fn sign<T>(mut request: Request<T>, token: &str) -> Result<Request<T>> {
    let mut parts = request.uri().clone().into_parts();
    let mut query: Vec<&str> = request
        .uri()
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|v| !v.is_empty())
        .filter(|v| {
            form_urlencoded::parse(v.as_bytes())
                .next()
                .is_none_or(|(name, _)| name != "access_token")
        })
        .collect();
    let encoded = form_urlencoded::Serializer::new(String::new())
        .append_pair("access_token", token)
        .finish();
    query.push(&encoded);
    parts.path_and_query = Some(
        format!("{}?{}", request.uri().path(), query.join("&"))
            .parse()
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to encode Baidu authentication",
                )
            })?,
    );
    *request.uri_mut() = http::Uri::from_parts(parts)
        .map_err(|_| Error::new(ErrorKind::Unexpected, "failed to build authenticated URI"))?;
    request.headers_mut().insert(
        header::USER_AGENT,
        http::HeaderValue::from_static("pan.baidu.com"),
    );
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Response;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn signing_preserves_download_signature_and_replaces_old_tokens() {
        let request = Request::get(
            "https://d.pcs.baidu.com/file?sign=a%2Fb%2Bc&access_token=old&access%5Ftoken=older",
        )
        .body(())
        .unwrap();
        let signed = sign(request, "new+/=& token").unwrap();
        assert_eq!(
            signed.uri().query().unwrap(),
            "sign=a%2Fb%2Bc&access_token=new%2B%2F%3D%26+token"
        );
        let before = signed.uri().clone();
        let signed = sign(signed, "new+/=& token").unwrap();
        assert_eq!(*signed.uri(), before);
    }

    #[derive(Clone)]
    struct TokenServer {
        calls: Arc<AtomicUsize>,
        reject: bool,
    }
    impl HttpTransport for TokenServer {
        async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
            let count = self.calls.fetch_add(1, Ordering::SeqCst);
            let query: std::collections::HashMap<_, _> =
                form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
                    .into_owned()
                    .collect();
            assert_eq!(query["refresh_token"], format!("refresh-{count}"));
            assert_eq!(query["client_secret"], "secret+/=&");
            tokio::task::yield_now().await;
            let body = if self.reject {
                serde_json::json!({"error":"invalid_grant", "error_description":"secret+/=&"})
            } else {
                serde_json::json!({"access_token":format!("access-{}",count+1),"refresh_token":format!("refresh-{}",count+1),"expires_in":3600})
            };
            let body = Buffer::from(body.to_string());
            let len = body.len() as u64;
            Ok(Response::new(HttpBody::new(
                futures::stream::iter(vec![Ok(body)]),
                Some(len),
            )))
        }
    }

    fn config() -> BaiduNetdiskConfig {
        BaiduNetdiskConfig {
            client_id: Some("app".into()),
            client_secret: Some("secret+/=&".into()),
            refresh_token: Some("refresh-0".into()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn concurrent_refresh_rotates_and_retries_persistence_without_reusing_token() {
        let calls = Arc::new(AtomicUsize::new(0));
        let saves = Arc::new(AtomicUsize::new(0));
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(TokenServer {
            calls: calls.clone(),
            reject: false,
        }));
        let saves_clone = saves.clone();
        let signer = BaiduNetdiskSigner::new(
            &config(),
            Some(Arc::new(move |access, refresh| {
                let count = saves_clone.fetch_add(1, Ordering::SeqCst);
                if count == 0 {
                    return Err(Error::new(ErrorKind::Unexpected, "storage unavailable"));
                }
                assert!(matches!(
                    (access, refresh),
                    ("access-1", "refresh-1") | ("access-2", "refresh-2")
                ));
                Ok(())
            })),
        )
        .unwrap();
        assert!(signer.token(&ctx).await.is_err());
        let (a, b) = tokio::join!(signer.token(&ctx), signer.token(&ctx));
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(saves.load(Ordering::SeqCst), 2);
        assert!(signer.invalidate("access-1").await);
        assert_eq!(signer.token(&ctx).await.unwrap(), "access-2");
        assert!(signer.invalidate("access-1").await);
        assert_eq!(signer.token(&ctx).await.unwrap(), "access-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_refresh_is_redacted_and_never_replayed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(TokenServer {
            calls: calls.clone(),
            reject: true,
        }));
        let signer = BaiduNetdiskSigner::new(&config(), None).unwrap();
        let error = signer.token(&ctx).await.unwrap_err();
        assert!(!format!("{error:?}").contains("secret+/=&"));
        assert!(signer.token(&ctx).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
