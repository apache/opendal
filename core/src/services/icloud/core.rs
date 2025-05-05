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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use http::header;
use http::header::IF_MATCH;
use http::header::IF_NONE_MATCH;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;

use crate::raw::*;
use crate::*;

static ACCOUNT_COUNTRY_HEADER: &str = "X-Apple-ID-Account-Country";
static OAUTH_STATE_HEADER: &str = "X-Apple-OAuth-State";
static SESSION_ID_HEADER: &str = "X-Apple-ID-Session-Id";
static SCNT_HEADER: &str = "scnt";
static SESSION_TOKEN_HEADER: &str = "X-Apple-Session-Token";
static APPLE_RESPONSE_HEADER: &str = "X-Apple-I-Rscd";

static AUTH_ENDPOINT: &str = "https://idmsa.apple.com/appleauth/auth";
static SETUP_ENDPOINT: &str = "https://setup.icloud.com/setup/ws/1";

const AUTH_HEADERS: [(&str, &str); 7] = [
    (
        // This code inspire from
        // https://github.com/picklepete/pyicloud/blob/master/pyicloud/base.py#L392
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

#[derive(Clone)]
pub struct SessionData {
    oauth_state: String,
    session_id: Option<String>,
    session_token: Option<String>,

    scnt: Option<String>,
    account_country: Option<String>,
    cookies: BTreeMap<String, String>,
    drivews_url: String,
    docws_url: String,
}

impl Default for SessionData {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionData {
    pub fn new() -> SessionData {
        Self {
            oauth_state: format!("auth-{}", uuid::Uuid::new_v4()).to_string(),
            session_id: None,
            session_token: None,
            scnt: None,
            account_country: None,

            cookies: BTreeMap::default(),
            drivews_url: String::new(),
            docws_url: String::new(),
        }
    }
}

#[derive(Clone)]
pub struct IcloudSigner {
    pub info: Arc<AccessorInfo>,

    pub apple_id: String,
    pub password: String,
    pub is_china_mainland: bool,
    pub trust_token: Option<String>,
    pub ds_web_auth_token: Option<String>,

    pub data: SessionData,
    pub initiated: bool,
}

impl Debug for IcloudSigner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("icloud signer");
        de.field("is_china_mainland", &self.is_china_mainland);
        de.finish()
    }
}

impl IcloudSigner {
    /// Get the drivews_url from signer session data.
    /// Async await init finish.
    pub async fn drivews_url(&mut self) -> Result<&str> {
        self.init().await?;
        Ok(&self.data.drivews_url)
    }

    /// Get the docws_url from signer session data.
    /// Async await init finish.
    pub async fn docws_url(&mut self) -> Result<&str> {
        self.init().await?;
        Ok(&self.data.docws_url)
    }

    /// iCloud will use our oauth state as client id.
    pub fn client_id(&self) -> &str {
        &self.data.oauth_state
    }

    async fn init(&mut self) -> Result<()> {
        if self.initiated {
            return Ok(());
        }

        // Sign the auth endpoint first.
        let uri = format!("{}/signin?isRememberMeEnable=true", AUTH_ENDPOINT);
        let body = serde_json::to_vec(&json!({
            "accountName" : self.apple_id,
            "password" : self.password,
            "rememberMe": true,
            "trustTokens": [self.trust_token.clone().unwrap()],
        }))
        .map_err(new_json_serialize_error)?;

        let mut req = Request::post(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;
        self.sign(&mut req)?;

        let resp = self.info.http_client().send(req).await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        if let Some(rscd) = resp.headers().get(APPLE_RESPONSE_HEADER) {
            let status_code = StatusCode::from_bytes(rscd.as_bytes()).unwrap();
            if status_code != StatusCode::CONFLICT {
                return Err(parse_error(resp));
            }
        }

        // Setup to get the session id.
        let uri = format!("{}/accountLogin", SETUP_ENDPOINT);
        let body = serde_json::to_vec(&json!({
            "accountCountryCode": self.data.account_country.clone().unwrap_or_default(),
            "dsWebAuthToken":self.ds_web_auth_token.clone().unwrap_or_default(),
            "extended_login": true,
            "trustToken": self.trust_token.clone().unwrap_or_default(),}))
        .map_err(new_json_serialize_error)?;

        let mut req = Request::post(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;
        self.sign(&mut req)?;

        let resp = self.info.http_client().send(req).await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        // Update SessionData cookies.We need obtain `X-APPLE-WEBAUTH-USER` cookie to get file.
        self.update(&resp)?;

        let bs = resp.into_body();
        let auth_info: IcloudWebservicesResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        // Check if we have extra challenge to take.
        if auth_info.hsa_challenge_required && !auth_info.hsa_trusted_browser {
            return Err(Error::new(ErrorKind::Unexpected, "Apple icloud AuthenticationFailed:Unauthorized request:Needs two-factor authentication"));
        }

        if let Some(v) = &auth_info.webservices.drivews.url {
            self.data.drivews_url = v.to_string();
        }
        if let Some(v) = &auth_info.webservices.docws.url {
            self.data.docws_url = v.to_string();
        }

        self.initiated = true;
        Ok(())
    }

    fn sign<T>(&mut self, req: &mut Request<T>) -> Result<()> {
        let headers = req.headers_mut();

        headers.insert(
            OAUTH_STATE_HEADER,
            build_header_value(&self.data.oauth_state)?,
        );

        if let Some(session_id) = &self.data.session_id {
            headers.insert(SESSION_ID_HEADER, build_header_value(session_id)?);
        }
        if let Some(scnt) = &self.data.scnt {
            headers.insert(SCNT_HEADER, build_header_value(scnt)?);
        }

        // You can get more information from [apple.com](https://support.apple.com/en-us/111754)
        if self.is_china_mainland {
            headers.insert(
                header::ORIGIN,
                build_header_value("https://www.icloud.com.cn")?,
            );
            headers.insert(
                header::REFERER,
                build_header_value("https://www.icloud.com.cn/")?,
            );
        } else {
            headers.insert(
                header::ORIGIN,
                build_header_value("https://www.icloud.com")?,
            );
            headers.insert(
                header::REFERER,
                build_header_value("https://www.icloud.com/")?,
            );
        }

        if !self.data.cookies.is_empty() {
            let cookies: Vec<String> = self
                .data
                .cookies
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            headers.insert(
                header::COOKIE,
                build_header_value(&cookies.as_slice().join("; "))?,
            );
        }

        for (key, value) in AUTH_HEADERS {
            headers.insert(key, build_header_value(value)?);
        }

        Ok(())
    }

    /// Update signer's data after request sent out.
    fn update(&mut self, resp: &Response<Buffer>) -> Result<()> {
        if let Some(account_country) = parse_header_to_str(resp.headers(), ACCOUNT_COUNTRY_HEADER)?
        {
            self.data.account_country = Some(account_country.to_string());
        }
        if let Some(session_id) = parse_header_to_str(resp.headers(), SESSION_ID_HEADER)? {
            self.data.session_id = Some(session_id.to_string());
        }
        if let Some(session_token) = parse_header_to_str(resp.headers(), SESSION_TOKEN_HEADER)? {
            self.data.session_token = Some(session_token.to_string());
        }

        if let Some(scnt) = parse_header_to_str(resp.headers(), SCNT_HEADER)? {
            self.data.scnt = Some(scnt.to_string());
        }

        let cookies: Vec<String> = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect();

        for cookie in cookies {
            if let Some((key, value)) = cookie.split_once('=') {
                self.data.cookies.insert(key.into(), value.into());
            }
        }

        Ok(())
    }

    /// Send will make sure the following things:
    ///
    /// - Init the signer if it's not initiated.
    /// - Sign the request.
    /// - Update the session data if needed.
    pub async fn send(&mut self, mut req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.sign(&mut req)?;
        let resp = self.info.http_client().send(req).await?;

        Ok(resp)
    }
}

pub struct IcloudCore {
    pub info: Arc<AccessorInfo>,
    pub signer: Arc<Mutex<IcloudSigner>>,
    pub root: String,
    pub path_cache: PathCacher<IcloudPathQuery>,
}

impl Debug for IcloudCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("IcloudCore");
        de.field("root", &self.root);
        de.finish()
    }
}

impl IcloudCore {
    // Retrieves a root within the icloud Drive.
    // "FOLDER::com.apple.CloudDocs::root"
    pub async fn get_root(&self, id: &str) -> Result<IcloudRoot> {
        let mut signer = self.signer.lock().await;

        let uri = format!(
            "{}/retrieveItemDetailsInFolders",
            signer.drivews_url().await?
        );

        let body = serde_json::to_vec(&json!([
             {
                 "drivewsid": id,
                 "partialData": false
             }
        ]))
        .map_err(new_json_serialize_error)?;

        let req = Request::post(uri)
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        let resp = signer.send(req).await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let body = resp.into_body();
        let drive_node: Vec<IcloudRoot> =
            serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;
        Ok(drive_node[0].clone())
    }

    pub async fn get_file(
        &self,
        id: &str,
        zone: &str,
        range: BytesRange,
        args: OpRead,
    ) -> Result<Response<HttpBody>> {
        let mut signer = self.signer.lock().await;

        let uri = format!(
            "{}/ws/{}/download/by_id?document_id={}",
            signer.docws_url().await?,
            zone,
            id
        );

        let req = Request::get(uri)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = signer.send(req).await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let body = resp.into_body();
        let object: IcloudObject =
            serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;

        let url = object.data_token.url.to_string();

        let mut req = Request::get(url);

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }

        if range.is_full() {
            req = req.header(header::RANGE, range.to_header())
        }
        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.info.http_client().fetch(req).await?;

        Ok(resp)
    }

    pub async fn read(
        &self,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let path = build_rooted_abs_path(&self.root, path);
        let base = get_basename(&path);

        let path_id = self.path_cache.get(base).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            format!("read path not found: {}", base),
        ))?;

        if let Some(docwsid) = path_id.strip_prefix("FILE::com.apple.CloudDocs::") {
            Ok(self
                .get_file(docwsid, "com.apple.CloudDocs", range, args.clone())
                .await?)
        } else {
            Err(Error::new(
                ErrorKind::NotFound,
                "icloud DriveService read error",
            ))
        }
    }

    pub async fn stat(&self, path: &str) -> Result<IcloudItem> {
        let path = build_rooted_abs_path(&self.root, path);

        let mut base = get_basename(&path);
        let parent = get_parent(&path);

        if base.ends_with('/') {
            base = base.trim_end_matches('/');
        }

        let file_id = self.path_cache.get(base).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            format!("stat path not found: {}", base),
        ))?;

        let folder_id = self.path_cache.get(parent).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            format!("stat path not found: {}", parent),
        ))?;

        let node = self.get_root(&folder_id).await?;

        match node.items.iter().find(|it| it.drivewsid == file_id.clone()) {
            Some(it) => Ok(it.clone()),
            None => Err(Error::new(
                ErrorKind::NotFound,
                "icloud DriveService stat get parent items error",
            )),
        }
    }
}

pub struct IcloudPathQuery {
    pub signer: Arc<Mutex<IcloudSigner>>,
}

impl IcloudPathQuery {
    pub fn new(signer: Arc<Mutex<IcloudSigner>>) -> Self {
        IcloudPathQuery { signer }
    }
}

impl PathQuery for IcloudPathQuery {
    async fn root(&self) -> Result<String> {
        Ok("FOLDER::com.apple.CloudDocs::root".to_string())
    }

    /// Retrieves the root directory within the icloud Drive.
    ///
    /// FIXME: we are reading the entire dir to find the file, this is not efficient.
    /// Maybe we should build a new path cache for this kind of services instead.
    async fn query(&self, parent_id: &str, name: &str) -> Result<Option<String>> {
        let mut signer = self.signer.lock().await;

        let uri = format!(
            "{}/retrieveItemDetailsInFolders",
            signer.drivews_url().await?
        );

        let body = serde_json::to_vec(&json!([
             {
                 "drivewsid": parent_id,
                 "partialData": false
             }
        ]))
        .map_err(new_json_serialize_error)?;

        let req = Request::post(uri)
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        let resp = signer.send(req).await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let body = resp.into_body();
        let root: Vec<IcloudRoot> =
            serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;

        let node = &root[0];

        Ok(node
            .items
            .iter()
            .find(|it| it.name == name)
            .map(|it| it.drivewsid.clone()))
    }

    async fn create_dir(&self, parent_id: &str, name: &str) -> Result<String> {
        let mut signer = self.signer.lock().await;

        let client_id = signer.client_id().to_string();

        let uri = format!("{}/createFolders", signer.drivews_url().await?);
        let body = serde_json::to_vec(&json!(
             {
                 "destinationDrivewsId": parent_id,
                 "folders": [
                 {
                    "clientId": client_id,
                    "name": name,
                }
                ],
             }
        ))
        .map_err(new_json_serialize_error)?;

        let req = Request::post(uri)
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        let resp = signer.send(req).await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let body = resp.into_body();
        let create_folder: IcloudCreateFolder =
            serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;
        Ok(create_folder.destination_drivews_id)
    }
}

pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let mut kind = match parts.status.as_u16() {
        421 | 450 | 500 => ErrorKind::NotFound,
        401 => ErrorKind::Unexpected,
        _ => ErrorKind::Unexpected,
    };

    let (message, icloud_err) = serde_json::from_reader::<_, IcloudError>(bs.clone().reader())
        .map(|icloud_err| (format!("{icloud_err:?}"), Some(icloud_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(icloud_err) = &icloud_err {
        kind = match icloud_err.status_code.as_str() {
            "NOT_FOUND" => ErrorKind::NotFound,
            "PERMISSION_DENIED" => ErrorKind::PermissionDenied,
            _ => ErrorKind::Unexpected,
        }
    }

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    err
}

#[derive(Default, Debug, Deserialize)]
#[allow(dead_code)]
struct IcloudError {
    status_code: String,
    message: String,
}

#[derive(Default, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IcloudWebservicesResponse {
    #[serde(default)]
    pub hsa_challenge_required: bool,
    #[serde(default)]
    pub hsa_trusted_browser: bool,
    pub webservices: Webservices,
}

#[derive(Deserialize, Default, Clone, Debug)]
pub struct Webservices {
    pub drivews: Drivews,
    pub docws: Docws,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Drivews {
    pub url: Option<String>,
}

#[derive(Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Docws {
    pub url: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IcloudRoot {
    #[serde(default)]
    pub asset_quota: i64,
    #[serde(default)]
    pub date_created: String,
    #[serde(default)]
    pub direct_children_count: i64,
    pub docwsid: String,
    pub drivewsid: String,
    pub etag: String,
    #[serde(default)]
    pub file_count: i64,
    pub items: Vec<IcloudItem>,
    pub name: String,
    pub number_of_items: i64,
    pub status: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub zone: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IcloudItem {
    #[serde(default)]
    pub asset_quota: Option<i64>,
    #[serde(default)]
    pub date_created: String,
    #[serde(default)]
    pub date_modified: String,
    #[serde(default)]
    pub direct_children_count: Option<i64>,
    pub docwsid: String,
    pub drivewsid: String,
    pub etag: String,
    #[serde(default)]
    pub file_count: Option<i64>,
    pub item_id: Option<String>,
    pub name: String,
    pub parent_id: String,
    #[serde(default)]
    pub size: u64,
    #[serde(rename = "type")]
    pub type_field: String,
    pub zone: String,
    #[serde(default)]
    pub max_depth: Option<String>,
    #[serde(default)]
    pub is_chained_to_parent: Option<bool>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcloudObject {
    pub document_id: String,
    pub item_id: String,
    pub owner_dsid: i64,
    pub data_token: DataToken,
    pub double_etag: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataToken {
    pub url: String,
    pub token: String,
    pub signature: String,
    pub wrapping_key: String,
    pub reference_signature: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcloudCreateFolder {
    pub destination_drivews_id: String,
    pub folders: Vec<IcloudItem>,
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::IcloudRoot;
    use super::IcloudWebservicesResponse;

    #[test]
    fn test_parse_icloud_drive_root_json() {
        let data = r#"{
          "assetQuota": 19603579,
          "dateCreated": "2019-06-10T14:17:49Z",
          "directChildrenCount": 3,
          "docwsid": "root",
          "drivewsid": "FOLDER::com.apple.CloudDocs::root",
          "etag": "w7",
          "fileCount": 22,
          "items": [
            {
              "assetQuota": 19603579,
              "dateCreated": "2021-02-05T08:30:58Z",
              "directChildrenCount": 22,
              "docwsid": "1E013608-C669-43DB-AC14-3D7A4E0A0500",
              "drivewsid": "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500",
              "etag": "sn",
              "fileCount": 22,
              "item_id": "CJWdk48eEAAiEB4BNgjGaUPbrBQ9ek4KBQAoAQ",
              "name": "Downloads",
              "parentId": "FOLDER::com.apple.CloudDocs::root",
              "shareAliasCount": 0,
              "shareCount": 0,
              "type": "FOLDER",
              "zone": "com.apple.CloudDocs"
            },
            {
              "dateCreated": "2019-06-10T14:17:54Z",
              "docwsid": "documents",
              "drivewsid": "FOLDER::com.apple.Keynote::documents",
              "etag": "1v",
              "maxDepth": "ANY",
              "name": "Keynote",
              "parentId": "FOLDER::com.apple.CloudDocs::root",
              "type": "APP_LIBRARY",
              "zone": "com.apple.Keynote"
            },
            {
              "assetQuota": 0,
              "dateCreated": "2024-01-06T02:35:08Z",
              "directChildrenCount": 0,
              "docwsid": "21E4A15E-DA77-472A-BAC8-B0C35A91F237",
              "drivewsid": "FOLDER::com.apple.CloudDocs::21E4A15E-DA77-472A-BAC8-B0C35A91F237",
              "etag": "w8",
              "fileCount": 0,
              "isChainedToParent": true,
              "item_id": "CJWdk48eEAAiECHkoV7ad0cqusiww1qR8jcoAQ",
              "name": "opendal",
              "parentId": "FOLDER::com.apple.CloudDocs::root",
              "shareAliasCount": 0,
              "shareCount": 0,
              "type": "FOLDER",
              "zone": "com.apple.CloudDocs"
            }
          ],
          "name": "",
          "numberOfItems": 16,
          "shareAliasCount": 0,
          "shareCount": 0,
          "status": "OK",
          "type": "FOLDER",
          "zone": "com.apple.CloudDocs"
        }"#;

        let response: IcloudRoot = serde_json::from_str(data).unwrap();
        assert_eq!(response.name, "");
        assert_eq!(response.type_field, "FOLDER");
        assert_eq!(response.zone, "com.apple.CloudDocs");
        assert_eq!(response.docwsid, "root");
        assert_eq!(response.drivewsid, "FOLDER::com.apple.CloudDocs::root");
        assert_eq!(response.etag, "w7");
        assert_eq!(response.file_count, 22);
    }

    #[test]
    fn test_parse_icloud_drive_folder_file() {
        let data = r#"{
          "assetQuota": 19603579,
          "dateCreated": "2021-02-05T08:34:21Z",
          "directChildrenCount": 22,
          "docwsid": "1E013608-C669-43DB-AC14-3D7A4E0A0500",
          "drivewsid": "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500",
          "etag": "w9",
          "fileCount": 22,
          "items": [
            {
              "dateChanged": "2021-02-18T14:10:46Z",
              "dateCreated": "2021-02-10T07:01:34Z",
              "dateModified": "2021-02-10T07:01:34Z",
              "docwsid": "9605331E-7BF3-41A0-A128-A68FFA377C50",
              "drivewsid": "FILE::com.apple.CloudDocs::9605331E-7BF3-41A0-A128-A68FFA377C50",
              "etag": "5b::5a",
              "extension": "pdf",
              "item_id": "CJWdk48eEAAiEJYFMx5780GgoSimj_o3fFA",
              "lastOpenTime": "2021-02-10T10:28:42Z",
              "name": "1-11-ARP-notes",
              "parentId": "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500",
              "size": 639483,
              "type": "FILE",
              "zone": "com.apple.CloudDocs"
            }
            ],
          "name": "Downloads",
          "numberOfItems": 22,
          "parentId": "FOLDER::com.apple.CloudDocs::root",
          "shareAliasCount": 0,
          "shareCount": 0,
          "status": "OK",
          "type": "FOLDER",
          "zone": "com.apple.CloudDocs"
        }"#;

        let response = serde_json::from_str::<IcloudRoot>(data).unwrap();

        assert_eq!(response.name, "Downloads");
        assert_eq!(response.type_field, "FOLDER");
        assert_eq!(response.zone, "com.apple.CloudDocs");
        assert_eq!(response.docwsid, "1E013608-C669-43DB-AC14-3D7A4E0A0500");
        assert_eq!(
            response.drivewsid,
            "FOLDER::com.apple.CloudDocs::1E013608-C669-43DB-AC14-3D7A4E0A0500"
        );
        assert_eq!(response.etag, "w9");
        assert_eq!(response.file_count, 22);
    }

    #[test]
    fn test_parse_icloud_webservices() {
        let data = r#"
        {
          "hsaChallengeRequired": false,
          "hsaTrustedBrowser": true,
          "webservices": {
            "docws": {
              "pcsRequired": true,
              "status": "active",
              "url": "https://p219-docws.icloud.com.cn:443"
            },
            "drivews": {
              "pcsRequired": true,
              "status": "active",
              "url": "https://p219-drivews.icloud.com.cn:443"
            }
          }
        }
        "#;
        let response = serde_json::from_str::<IcloudWebservicesResponse>(data).unwrap();
        assert!(!response.hsa_challenge_required);
        assert!(response.hsa_trusted_browser);
        assert_eq!(
            response.webservices.docws.url,
            Some("https://p219-docws.icloud.com.cn:443".to_string())
        );
        assert_eq!(
            response.webservices.drivews.url,
            Some("https://p219-drivews.icloud.com.cn:443".to_string())
        );
    }
}
