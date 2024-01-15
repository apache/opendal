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

use chrono::Utc;
use http::header::{IF_MATCH, IF_NONE_MATCH};
use http::{Method, Request, Response, StatusCode};
use log::debug;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::raw::oio::WriteBuf;
use crate::raw::{new_json_deserialize_error, AsyncBody, IncomingAsyncBody, OpRead};
use crate::{Error, ErrorKind, Result};

use super::core::{
    parse_error, IcloudCreateFolder, IcloudItem, IcloudObject, IcloudRoot, IcloudSigner,
};

#[derive(Clone)]
pub struct File {
    pub id: Option<String>,
    pub name: String,
    pub size: u64,
    pub date_created: Option<chrono::DateTime<Utc>>,
    pub date_modified: Option<chrono::DateTime<Utc>>,
    pub mime_type: String,
}

// A directory in iCloud Drive.
#[derive(Clone)]
pub struct Folder {
    pub id: Option<String>,
    pub name: String,
    pub date_created: Option<chrono::DateTime<Utc>>,
    pub items: Vec<IcloudItem>,
    pub mime_type: String,
}

pub struct FolderIter<'a> {
    current: std::slice::Iter<'a, IcloudItem>,
}

impl<'a> Iterator for FolderIter<'a> {
    type Item = &'a IcloudItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.current.next()
    }
}

impl Folder {
    #[warn(dead_code)]
    pub fn iter(&self) -> FolderIter {
        FolderIter {
            current: self.items.iter(),
        }
    }
}

// A node within the iCloud Drive filesystem.
#[derive(Clone)]
#[warn(unused_variables)]
#[warn(dead_code)]
pub enum DriveNode {
    Folder(Folder),
    File(File),
}

impl DriveNode {
    fn new_root(value: &IcloudRoot) -> Result<DriveNode> {
        Ok(DriveNode::Folder(Folder {
            id: Option::from(value.drivewsid.to_string()),
            name: value.name.to_string(),
            date_created: value.date_created.parse::<chrono::DateTime<Utc>>().ok(),
            items: value.items.clone(),
            mime_type: "Folder".to_string(),
        }))
    }

    #[warn(dead_code)]
    pub fn id(&self) -> Option<String> {
        match self {
            DriveNode::Folder(folder) => folder.id.clone(),
            DriveNode::File(file) => file.id.clone(),
        }
    }
    #[warn(dead_code)]
    pub fn name(&self) -> &String {
        match self {
            DriveNode::Folder(folder) => &folder.name,
            DriveNode::File(file) => &file.name,
        }
    }
    #[warn(dead_code)]
    pub fn size(&self) -> Option<u64> {
        match self {
            #[warn(unused_variables)]
            DriveNode::Folder(folder) => None,
            DriveNode::File(file) => Some(file.size),
        }
    }
    #[warn(dead_code)]
    pub fn mime_type(&self) -> String {
        match self {
            DriveNode::Folder(folder) => folder.mime_type.clone(),
            DriveNode::File(file) => file.mime_type.clone(),
        }
    }

    pub fn items(&self) -> Option<Vec<IcloudItem>> {
        match self {
            DriveNode::Folder(folder) => Some(folder.items.clone()),
            DriveNode::File(file) => None,
        }
    }
}

impl std::fmt::Display for DriveNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            DriveNode::Folder(folder) => {
                write!(
                    f,
                    "Folder(id={:?},name={},dateCreated={:?},items={})",
                    folder.id,
                    folder.name,
                    folder.date_created,
                    folder.items.len()
                )
            }
            DriveNode::File(file) => {
                write!(
                    f,
                    "File(id={:?},name={},dateCreated={:?},dataModified={:?},size={})",
                    file.id, file.name, file.date_created, file.date_modified, file.size,
                )
            }
        }
    }
}

pub struct DriveService {
    core: Arc<Mutex<IcloudSigner>>,
    drive_url: String,
    docw_url: String,
}

impl DriveService {
    // Constructs an interface to an iCloud Drive.
    pub fn new(
        core: Arc<Mutex<IcloudSigner>>,
        drive_url: String,
        docw_url: String,
    ) -> DriveService {
        DriveService {
            core,
            drive_url,
            docw_url,
        }
    }

    // Retrieves the root directory within the iCloud Drive.
    pub async fn root(&self) -> Result<Folder> {
        match self.get_root("FOLDER::com.apple.CloudDocs::root").await? {
            DriveNode::Folder(folder) => Ok(folder),
            _ => Err(Error::new(ErrorKind::Unexpected, "InvalidDriveNodeType")),
        }
    }

    // Retrieves a root within the iCloud Drive.
    //"FOLDER::com.apple.CloudDocs::root"
    pub async fn get_root(&self, id: &str) -> Result<DriveNode> {
        let uri = format!("{}/retrieveItemDetailsInFolders", self.drive_url);

        let body = json!([
                         {
                             "drivewsid": id,
                             "partialData": false
                         }
        ])
        .to_string();

        let mut core = self.core.lock().await;
        let async_body = AsyncBody::Bytes(bytes::Bytes::from(body));

        let response = core.sign(Method::POST, uri, async_body).await?;

        if response.status() == StatusCode::OK {
            let body = &response.into_body().bytes().await?;

            //iCloudRoot
            let drive_node: Vec<IcloudRoot> =
                serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;

            Ok(DriveNode::new_root(&drive_node[0])?)
        } else {
            Err(parse_error(response).await?)
        }
    }

    pub async fn get_file(
        &self,
        id: &str,
        zone: &str,
        args: OpRead,
    ) -> Result<Response<IncomingAsyncBody>> {
        //https://p219-docws.icloud.com.cn:443
        let uri = format!(
            "{}\
        /ws/{}/download/by_id?document_id={}",
            self.docw_url, zone, id
        );
        debug!("{}", uri);

        let mut core = self.core.lock().await;

        let response = core.sign(Method::GET, uri, AsyncBody::Empty).await?;

        match response.status() {
            StatusCode::OK => {
                let body = &response.into_body().bytes().await?;
                let object: IcloudObject =
                    serde_json::from_slice(body.chunk()).map_err(new_json_deserialize_error)?;

                let url = object.data_token.url.to_string();

                let mut request_builder = Request::builder().method(Method::GET).uri(url);

                if let Some(if_match) = args.if_match() {
                    request_builder = request_builder.header(IF_MATCH, if_match);
                }

                let range = args.range();
                if !range.is_full() {
                    request_builder = request_builder.header(http::header::RANGE, range.to_header())
                }

                if let Some(if_none_match) = args.if_none_match() {
                    request_builder = request_builder.header(IF_NONE_MATCH, if_none_match);
                }

                let async_body = request_builder.body(AsyncBody::Empty).unwrap();

                let response = core.client.send(async_body).await?;

                Ok(response)
            }
            _ => Err(parse_error(response).await?),
        }
    }

    #[warn(dead_code)]
    pub async fn create_folder(&self, parent_id: &str, name: &str) -> Result<String> {
        let mut core = self.core.lock().await;
        let client_id = core.get_client_id();
        let uri = format!("{}/createFolders", self.drive_url);
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
