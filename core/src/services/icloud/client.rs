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

use crate::raw::{build_abs_path, build_rooted_abs_path, get_basename, IncomingAsyncBody, OpRead};
use std::collections::HashMap;
use std::sync::Arc;
use http::Response;
use tokio::sync::Mutex;

use crate::services::icloud::drive::DriveService;
use crate::services::icloud::session::Session;
use crate::{Error, ErrorKind, Result};
use crate::services::icloud::graph_model::iCloudItem;

#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) session: Arc<Mutex<Session>>,
    pub root: String,
    pub path_cache: Arc<Mutex<HashMap<String, String>>>,
}

impl Client {
    // Logs into iCloud using the provided credentials.
    pub async fn login(&self) -> Result<()> {
        let mut session = self.session.lock().await;

        session.sign().await
    }

    //APPLE Drive
    pub async fn drive(&self) -> Option<DriveService> {
        let clone = self.session.clone();
        let session = self.session.lock().await;

        let docw=session.get_service_info(String::from("docw")).unwrap();
        session
            .get_service_info(String::from("drive"))
            .map(|s| DriveService::new(clone, s.url.clone(),docw.url.clone()))
    }

    pub async fn get_stat_folder_id(&self,path:&str) -> Result<String> {
        let path =build_rooted_abs_path(&self.root, path);
        let mut base=get_basename(&path);

        if base.ends_with("/") {
            base=base.trim_end_matches("/");
        }
        let cache = self.path_cache.lock().await;
        if let Some(id) = cache.get(base) {
            Ok(id.to_owned())
        } else {
            Ok("".to_string())
        }
    }

    pub async fn get_file_id_by_path(&self,path:&str) -> Result<Option<String>> {
        let path =build_abs_path(&self.root, path);

        let mut cache = self.path_cache.lock().await;

        if let Some(id) = cache.get(&path) {
            return Ok(Some(id.to_owned()));
        }

        let drive =self.drive().await.unwrap();

        let root=drive.root().await?;

        let mut parent_id=root.id.to_owned().unwrap();


        let mut file_path_items: Vec<&str> = path.split('/').filter(|&x| !x.is_empty()).collect();
       file_path_items.remove(0);

        for (i, item) in file_path_items.iter().enumerate() {
            let path_part = file_path_items[0..=i].join("/");
            if let Some(id) = cache.get(&path_part) {
                parent_id=self.get_id(&id, item, &drive).await?.unwrap();
                continue;
            }

            let id = if i != file_path_items.len() - 1 || path.ends_with('/') {
                self.get_parent_id(&parent_id, item, & drive).await?
            } else {
                self.get_parent_id(&parent_id, item,& drive).await?
            };



            if let Some(id) = id {
                cache.insert(path_part, id.clone());
                parent_id=self.get_id(&parent_id, item, &drive).await?.unwrap();
            } else {
                return Ok(None);
            };
        }

        Ok(Some(parent_id))
    }

    pub async fn get_parent_id(& self, parent_id:&str, basename:&str, drive: &DriveService) -> Result<Option<String>> {
        let node=drive.get_root(parent_id).await?;

        match node.items() {
            Some(items) => {
                match items.iter().find(|it| it.name == basename) {
                    Some(it) => Ok(Some(it.parent_id.clone())),
                    None => Ok(None),
                }
            }
            None => {
                Err(Error::new(ErrorKind::NotFound,"get parent id error"))
            }
        }
    }


    pub async fn get_id(&self, parent_id:&str, basename:&str, drive: &DriveService) -> Result<Option<String>> {
        let node=drive.get_root(parent_id).await?;

        match node.items() {
            Some(items) => {
                match items.iter().find(|it| it.name == basename) {
                    Some(it) => Ok(Some(it.drivewsid.clone())),
                    None => Ok(None),
                }
            }
            None => {
                Err(Error::new(ErrorKind::NotFound,"get parent id error"))
            }
        }
    }

    pub async fn read(&self,path:&str, args: &OpRead) -> Result<Response<IncomingAsyncBody>> {
        self.login().await?;
        let drive =self.drive().await.expect("iCloud DriveService read drive not found");
        let path_id = self.get_file_id_by_path(path).await.expect("iCloud DriveService read path_id not found");


        if let Some(docwsid)=path_id.unwrap().strip_prefix("FILE::com.apple.CloudDocs::") {
            Ok(drive.get_file(docwsid, "com.apple.CloudDocs", args.clone()).await?)
        } else {
            Err(Error::new(ErrorKind::NotFound,"iCloud DriveService read error"))
        }
    }

    pub async fn stat(&self, path: & str) -> Result<iCloudItem> {
        self.login().await?;
        let path_id = self.get_file_id_by_path(path).await.expect("iCloud DriveService stat path_id don't find");

        let drive = self.drive().await.expect("iCloud DriveService stat drive not found");

        let folder_id = self.get_stat_folder_id(path).await.expect("iCloud DriveService stat folder_id don't find");
        if folder_id.is_empty() {
            return Err(Error::new(ErrorKind::NotFound,"iCloud DriveService stat not exist path"));
        }

        let node = drive.get_root(&folder_id).await?;


        match node.items() {
            Some(items) => {
                match items.iter().find(|it| it.drivewsid == path_id.clone().unwrap().to_string()) {
                    Some(it) => Ok(it.clone()),
                    _ => Err(Error::new(
                        ErrorKind::NotFound,
                        "iCloud DriveService stat parent items don't have same drivewsid",
                    ))
                }
            }
            None => {
                Err(Error::new(
                    ErrorKind::NotFound,
                    "iCloud DriveService stat get parent items error",
                ))
            }
        }
    }

}
