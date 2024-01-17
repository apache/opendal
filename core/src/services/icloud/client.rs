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

use async_trait::async_trait;
use http::{Method, Response, StatusCode};
use serde_json::json;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::raw::oio::WriteBuf;
use crate::raw::*;
use crate::{Error, ErrorKind, Result};

use super::core::{parse_error, IcloudCreateFolder, IcloudItem, IcloudRoot, IcloudSigner};
use super::drive::DriveService;

pub struct Client {
    pub core: Arc<Mutex<IcloudSigner>>,
    pub root: String,
    pub path_cache: PathCacher<IcloudPathQuery>,
}

impl Debug for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("IcloudClient");
        de.field("root", &self.root);
        de.finish()
    }
}

pub struct IcloudPathQuery {
    pub client: HttpClient,
    pub core: Arc<Mutex<IcloudSigner>>,
}

impl IcloudPathQuery {
    pub fn new(client: HttpClient, core: Arc<Mutex<IcloudSigner>>) -> Self {
        IcloudPathQuery { client, core }
    }
}

#[async_trait]
impl PathQuery for IcloudPathQuery {
    async fn root(&self) -> Result<String> {
        Ok("FOLDER::com.apple.CloudDocs::root".to_string())
    }

    // Retrieves the root directory within the icloud Drive.
    async fn query(&self, parent_id: &str, name: &str) -> Result<Option<String>> {
        let mut core = self.core.lock().await;
        let drive_url = core
            .get_service_info(String::from("drive"))
            .unwrap()
            .clone()
            .url;

        let uri = format!("{}/retrieveItemDetailsInFolders", drive_url);

        let body = json!([
                         {
                             "drivewsid": parent_id,
                             "partialData": false
                         }
        ])
        .to_string();

        let async_body = AsyncBody::Bytes(bytes::Bytes::from(body));

        let response = core.sign(Method::POST, uri, async_body).await?;

        if response.status() == StatusCode::OK {
            let body = &response.into_body().bytes().await?;

            let root: Vec<IcloudRoot> =
                serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;

            let node = &root[0];

            let id = match node.items.iter().find(|it| it.name == name) {
                Some(it) => Ok(Some(it.drivewsid.clone())),
                None => Ok(None),
            }?;
            Ok(id)
        } else {
            Err(parse_error(response).await?)
        }
    }

    async fn create_dir(&self, parent_id: &str, name: &str) -> Result<String> {
        let mut core = self.core.lock().await;
        let client_id = core.get_client_id();
        let drive_url = core
            .get_service_info(String::from("drive"))
            .unwrap()
            .url
            .clone();
        // https://p219-drivews.icloud.com.cn:443
        let uri = format!("{}/createFolders", drive_url);
        let body = json!(
                         {
                             "destinationDrivewsId": parent_id,
                             "folders": [
                             {
                                "clientId": client_id,
                                "name": name,
                            }
                            ],
                         }
        )
        .to_string();

        let async_body = AsyncBody::Bytes(bytes::Bytes::from(body));
        let response = core.sign(Method::POST, uri, async_body).await?;

        match response.status() {
            StatusCode::OK => {
                let body = &response.into_body().bytes().await?;

                let create_folder: IcloudCreateFolder =
                    serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;

                Ok(create_folder.destination_drivews_id)
            }
            _ => Err(parse_error(response).await?),
        }
    }
}

impl Client {
    // Logs into icloud using the provided credentials.
    pub async fn login(&self) -> Result<()> {
        let mut core = self.core.lock().await;

        core.signer().await
    }

    //Apple Drive
    pub async fn drive(&self) -> Option<DriveService> {
        let clone = self.core.clone();
        let core = self.core.lock().await;

        let docw = core.get_service_info(String::from("docw")).unwrap();
        core.get_service_info(String::from("drive"))
            .map(|s| DriveService::new(clone, s.url.clone(), docw.url.clone()))
    }

    pub async fn read(&self, path: &str, args: &OpRead) -> Result<Response<IncomingAsyncBody>> {
        self.login().await?;

        let path = build_rooted_abs_path(&self.root, path);
        let base = get_basename(&path);

        let path_id = self.path_cache.get(base).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            &format!("read path not found: {}", base),
        ))?;

        let drive = self
            .drive()
            .await
            .expect("icloud DriveService read drive not found");

        if let Some(docwsid) = path_id.strip_prefix("FILE::com.apple.CloudDocs::") {
            Ok(drive
                .get_file(docwsid, "com.apple.CloudDocs", args.clone())
                .await?)
        } else {
            Err(Error::new(
                ErrorKind::NotFound,
                "icloud DriveService read error",
            ))
        }
    }

    pub async fn stat(&self, path: &str) -> Result<IcloudItem> {
        self.login().await?;

        let path = build_rooted_abs_path(&self.root, path);

        let mut base = get_basename(&path);
        let parent = get_parent(&path);

        if base.ends_with('/') {
            base = base.trim_end_matches('/');
        }

        let file_id = self.path_cache.get(base).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            &format!("stat path not found: {}", base),
        ))?;

        let drive = self
            .drive()
            .await
            .expect("icloud DriveService stat drive not found");

        let folder_id = self.path_cache.get(parent).await?.ok_or(Error::new(
            ErrorKind::NotFound,
            &format!("stat path not found: {}", parent),
        ))?;

        let node = drive.get_root(&folder_id).await?;

        match node.items() {
            Some(items) => match items.iter().find(|it| it.drivewsid == file_id.clone()) {
                Some(it) => Ok(it.clone()),
                _ => Err(Error::new(
                    ErrorKind::NotFound,
                    "icloud DriveService stat parent items don't have same drivewsid",
                )),
            },
            None => Err(Error::new(
                ErrorKind::NotFound,
                "icloud DriveService stat get parent items error",
            )),
        }
    }
}
