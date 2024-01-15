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

use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};

use http::header;
use http::Method;
use http::Request;
use http::Response;
use http::StatusCode;

use crate::types::Result;
use crate::{Error, ErrorKind};

use crate::raw::oio::WriteBuf;
use crate::raw::{new_json_deserialize_error, AsyncBody, HttpClient, IncomingAsyncBody};
use crate::services::icloud::error::parse_error;
use crate::services::icloud::webservices::iCloudWebservicesResponse;
use serde_json::json;

//TODO It just support cn
//Other country shoule be https://www.icloud.com
//Need to improve?
const GLOBAL_HEADERS: [(&str, &str); 2] = [
    ("Origin", "https://www.icloud.com.cn"),
    ("Referer", "https://www.icloud.com.cn/"),
];

static ACCOUNT_COUNTRY_HEADER: &str = "X-Apple-ID-Account-Country";
static OAUTH_STATE_HEADER: &str = "X-Apple-OAuth-State";

static SESSION_ID_HEADER: &str = "X-Apple-ID-Session-Id";

static SCNT_HEADER: &str = "scnt";

static SESSION_TOKEN_HEADER: &str = "X-Apple-Session-Token";
static AUTH_ENDPOINT: &str = "https://idmsa.apple.com/appleauth/auth";
static SETUP_ENDPOINT: &str = "https://setup.icloud.com/setup/ws/1";

static APPLE_RESPONSE_HEADER: &str = "X-Apple-I-Rscd";

const AUTH_HEADERS: [(&str, &str); 7] = [
    (
        "X-Apple-OAuth-Client-Id",
        "d39ba9916b7251055b22c7f910e2ea796ee65e98b2ddecea8f5dde8d9d1a815d",
    ),
    ("X-Apple-OAuth-Client-Type", "firstPartyAuth"),
    ("X-Apple-OAuth-Redirect-URI", "https://www.icloud.com"),
    ("X-Apple-OAuth-Require-Grant-Code", "true"),
    ("X-Apple-OAuth-Response-Mode", "web_message"),
    ("X-Apple-OAuth-Response-Type", "code"),
    (
        "X-Apple-Widget-Key",
        "d39ba9916b7251055b22c7f910e2ea796ee65e98b2ddecea8f5dde8d9d1a815d",
    ),
];

pub struct ServiceInfo {
    pub(crate) url: String,
}

pub struct SessionData {
    oauth_state: String,
    session_id: Option<String>,
    session_token: Option<String>,

    scnt: Option<String>,
    account_country: Option<String>,

    cookies: BTreeMap<String, String>,
    webservices: HashMap<String, ServiceInfo>,
}

impl SessionData {
    pub fn new() -> SessionData {
        Self {
            oauth_state: format!("auth-{}", uuid::Uuid::new_v4().to_string()).to_string(),
            session_id: None,
            session_token: None,
            scnt: None,
            account_country: None,
            cookies: BTreeMap::new(),
            webservices: HashMap::new(),
        }
    }
}

pub struct Session {
    pub(crate) data: SessionData,
    pub(crate) client: HttpClient,
    pub(crate) apple_id: String,
    pub(crate) password: String,

    pub trust_token: Option<String>,
    pub ds_web_auth_token: Option<String>,
}

impl Debug for Session {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("iCloud Session");
        de.field("apple_id", &self.apple_id);
        de.field("password", &self.password);
        de.field("trust_token", &self.trust_token);
        de.field("ds_web_auth_token", &self.ds_web_auth_token);
        de.finish()
    }
}

impl Session {
    pub fn get_service_info(&self, name: String) -> Option<&ServiceInfo> {
        self.data.webservices.get(&name)
    }

    pub fn get_client_id(&self) -> &String {
        &self.data.oauth_state
    }
}

impl Session {
    pub async fn sign(&mut self) -> Result<()> {
        let body = json!({
            "accountName" : self.apple_id,
            "password" : self.password,
            "rememberMe": true,
            "trustTokens": [self.trust_token.clone().unwrap()],
        })
        .to_string();

        let uri = format!("{}/signin?isRememberMeEnable=true", AUTH_ENDPOINT);

        let async_body = AsyncBody::Bytes(bytes::Bytes::from(body));

        let response = self.request(Method::POST, uri, async_body).await?;

        let status = response.status();

        return match status {
            StatusCode::OK => {
                if let Some(rscd) = response.headers().get(APPLE_RESPONSE_HEADER) {
                    let status_code = StatusCode::from_bytes(rscd.as_bytes()).unwrap();
                    //409
                    if status_code != StatusCode::CONFLICT {
                        return Err(parse_error(response).await?);
                    }
                }
                self.authenticate().await
            }
            _ => Err(parse_error(response).await?),
        };
    }

    pub async fn authenticate(&mut self) -> Result<()> {
        let body = json!({
            "accountCountryCode": self.data.account_country.as_ref().unwrap_or(&String::new()),
            "dsWebAuthToken":self.ds_web_auth_token.as_ref().unwrap_or(&String::new()),
                    "extended_login": true,
                    "trustToken": self.trust_token.as_ref().unwrap_or(&String::new())
        })
        .to_string();

        let uri = format!("{}/accountLogin", SETUP_ENDPOINT);

        let async_body = AsyncBody::Bytes(bytes::Bytes::from(body));

        let response = self.request(Method::POST, uri, async_body).await?;

        let status = response.status();

        match status {
            StatusCode::OK => {
                let body = &response.into_body().bytes().await?;
                let auth_info: iCloudWebservicesResponse =
                    serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;

                if let Some(drivews_url) = &auth_info.webservices.drivews.url {
                    self.data.webservices.insert(
                        String::from("drive"),
                        ServiceInfo {
                            url: drivews_url.to_string(),
                        },
                    );
                }
                if let Some(docws_url) = &auth_info.webservices.docws.url {
                    self.data.webservices.insert(
                        String::from("docw"),
                        ServiceInfo {
                            url: docws_url.to_string(),
                        },
                    );
                }

                if auth_info.hsa_challenge_required == true {
                    if auth_info.hsa_trusted_browser == true {
                        Ok(())
                    } else {
                        Err(Error::new(ErrorKind::Unexpected, "Apple iCloud AuthenticationFailed:Unauthorized request:Needs two-factor authentication"))
                    }
                } else {
                    Ok(())
                }
            }
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                "Apple iCloud AuthenticationFailed:Unauthorized:Invalid token",
            )),
        }
    }
}

impl Session {
    pub async fn request(
        &mut self,
        method: Method,
        uri: String,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let mut request = Request::builder().method(method).uri(uri);

        request = request.header(OAUTH_STATE_HEADER, self.data.oauth_state.clone());

        if let Some(session_id) = &self.data.session_id {
            request = request.header(SESSION_ID_HEADER, session_id);
        }
        if let Some(scnt) = &self.data.scnt {
            request = request.header(SCNT_HEADER, scnt);
        }

        for (key, vaule) in GLOBAL_HEADERS {
            request = request.header(key, vaule);
        }

        if self.data.cookies.len() > 0 {
            let cookies: Vec<String> = self
                .data
                .cookies
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            request = request.header(header::COOKIE, cookies.as_slice().join("; "));
        }

        if let Some(headers) = request.headers_mut() {
            headers.insert("Content-Type", "application/json".parse().unwrap());
            headers.insert("Accept", "*/*".parse().unwrap());
            for (key, value) in AUTH_HEADERS {
                headers.insert(key, value.parse().unwrap());
            }
        }

        match self.client.send(request.body(body).unwrap()).await {
            Ok(response) => {
                if let Some(account_country) = response.headers().get(ACCOUNT_COUNTRY_HEADER) {
                    self.data.account_country =
                        Some(String::from(account_country.to_str().unwrap()));
                }
                if let Some(session_id) = response.headers().get(SESSION_ID_HEADER) {
                    self.data.session_id = Some(String::from(session_id.to_str().unwrap()));
                }
                if let Some(session_token) = response.headers().get(SESSION_TOKEN_HEADER) {
                    self.data.session_token = Some(String::from(session_token.to_str().unwrap()));
                }

                if let Some(scnt) = response.headers().get(SCNT_HEADER) {
                    self.data.scnt = Some(String::from(scnt.to_str().unwrap()));
                }

                for (key, value) in response.headers() {
                    if key == header::SET_COOKIE {
                        if let Some(cookie) = value.to_str().unwrap().split(";").next() {
                            if let Some((key, value)) = cookie.split_once("=") {
                                self.data
                                    .cookies
                                    .insert(String::from(key), String::from(value));
                            }
                        }
                    }
                }
                match response.status() {
                    StatusCode::UNAUTHORIZED => Err(parse_error(response).await?),
                    _ => Ok(response),
                }
            }
            _ => Err(Error::new(
                ErrorKind::Unexpected,
                "Apple iCloud AuthenticationFailed:Unauthorized request",
            )),
        }
    }
}
