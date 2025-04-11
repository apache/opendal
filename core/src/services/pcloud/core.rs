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

use bytes::Buf;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use super::error::parse_error;
use super::error::PcloudError;
use crate::raw::*;
use crate::*;

#[derive(Clone)]
pub struct PcloudCore {
    pub info: Arc<AccessorInfo>,

    /// The root of this core.
    pub root: String,
    /// The endpoint of this backend.
    pub endpoint: String,
    /// The username id of this backend.
    pub username: String,
    /// The password of this backend.
    pub password: String,
}

impl Debug for PcloudCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

impl PcloudCore {
    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }
}

impl PcloudCore {
    pub async fn get_file_link(&self, path: &str) -> Result<String> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "{}/getfilelink?path=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(&path),
            self.username,
            self.password
        );

        let req = Request::get(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: GetFileLinkResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                let result = resp.result;
                if result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                }

                if let Some(hosts) = resp.hosts {
                    if let Some(path) = resp.path {
                        if !hosts.is_empty() {
                            return Ok(format!("https://{}{}", hosts[0], path));
                        }
                    }
                }
                Err(Error::new(ErrorKind::Unexpected, "hosts is empty"))
            }
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn download(&self, url: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let req = Request::get(url);

        // set body
        let req = req
            .header(header::RANGE, range.to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.info.http_client().fetch(req).await
    }

    pub async fn ensure_dir_exists(&self, path: &str) -> Result<()> {
        let path = build_abs_path(&self.root, path);

        let paths = path.split('/').collect::<Vec<&str>>();

        for i in 0..paths.len() - 1 {
            let path = paths[..i + 1].join("/");
            let resp = self.create_folder_if_not_exists(&path).await?;

            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let bs = resp.into_body();
                    let resp: PcloudError =
                        serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                    let result = resp.result;
                    if result == 2010 || result == 2055 || result == 2002 {
                        return Err(Error::new(ErrorKind::NotFound, format!("{resp:?}")));
                    }
                    if result != 0 {
                        return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                    }

                    if result != 0 {
                        return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                    }
                }
                _ => return Err(parse_error(resp)),
            }
        }
        Ok(())
    }

    pub async fn create_folder_if_not_exists(&self, path: &str) -> Result<Response<Buffer>> {
        let url = format!(
            "{}/createfolderifnotexists?path=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(path),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn rename_file(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let url = format!(
            "{}/renamefile?path=/{}&topath=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(&from),
            percent_encode_path(&to),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn rename_folder(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);
        let url = format!(
            "{}/renamefolder?path=/{}&topath=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(&from),
            percent_encode_path(&to),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn delete_folder(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "{}/deletefolder?path=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(&path),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn delete_file(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = format!(
            "{}/deletefile?path=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(&path),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn copy_file(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let url = format!(
            "{}/copyfile?path=/{}&topath=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(&from),
            percent_encode_path(&to),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn copy_folder(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let url = format!(
            "{}/copyfolder?path=/{}&topath=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(&from),
            percent_encode_path(&to),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn stat(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let path = path.trim_end_matches('/');

        let url = format!(
            "{}/stat?path=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(path),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn upload_file(&self, path: &str, bs: Buffer) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let (name, path) = (get_basename(&path), get_parent(&path).trim_end_matches('/'));

        let url = format!(
            "{}/uploadfile?path=/{}&filename={}&username={}&password={}",
            self.endpoint,
            percent_encode_path(path),
            percent_encode_path(name),
            self.username,
            self.password
        );

        let req = Request::put(url);

        // set body
        let req = req.body(bs).map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn list_folder(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let path = normalize_root(&path);

        let path = path.trim_end_matches('/');

        let url = format!(
            "{}/listfolder?path={}&username={}&password={}",
            self.endpoint,
            percent_encode_path(path),
            self.username,
            self.password
        );

        let req = Request::get(url);

        // set body
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.send(req).await
    }
}

pub(super) fn parse_stat_metadata(content: StatMetadata) -> Result<Metadata> {
    let mut md = if content.isfolder {
        Metadata::new(EntryMode::DIR)
    } else {
        Metadata::new(EntryMode::FILE)
    };

    if let Some(size) = content.size {
        md.set_content_length(size);
    }

    md.set_last_modified(parse_datetime_from_rfc2822(&content.modified)?);

    Ok(md)
}

pub(super) fn parse_list_metadata(content: ListMetadata) -> Result<Metadata> {
    let mut md = if content.isfolder {
        Metadata::new(EntryMode::DIR)
    } else {
        Metadata::new(EntryMode::FILE)
    };

    if let Some(size) = content.size {
        md.set_content_length(size);
    }

    md.set_last_modified(parse_datetime_from_rfc2822(&content.modified)?);

    Ok(md)
}

#[derive(Debug, Deserialize)]
pub struct GetFileLinkResponse {
    pub result: u64,
    pub path: Option<String>,
    pub hosts: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct StatResponse {
    pub result: u64,
    pub metadata: Option<StatMetadata>,
}

#[derive(Debug, Deserialize)]
pub struct StatMetadata {
    pub modified: String,
    pub isfolder: bool,
    pub size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ListFolderResponse {
    pub result: u64,
    pub metadata: Option<ListMetadata>,
}

#[derive(Debug, Deserialize)]
pub struct ListMetadata {
    pub path: String,
    pub modified: String,
    pub isfolder: bool,
    pub size: Option<u64>,
    pub contents: Option<Vec<ListMetadata>>,
}
