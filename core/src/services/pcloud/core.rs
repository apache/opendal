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

use std::fmt::Debug;
use std::fmt::Formatter;

use bytes::Bytes;
use http::header;
use http::Request;
use http::StatusCode;
use serde::Deserialize;

use super::error::parse_error;
use super::error::PcloudError;
use crate::raw::*;
use crate::*;

#[derive(Clone)]
pub struct PcloudCore {
    /// The root of this core.
    pub root: String,
    /// The endpoint of this backend.
    pub endpoint: String,
    /// The username id of this backend.
    pub username: String,
    /// The password of this backend.
    pub password: String,

    pub client: HttpClient,
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let resp: GetFileLinkResponse = body.to_json().await?;
                let result = resp.result;
                if result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, &format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
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
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn download(
        &self,
        url: &str,
        range: BytesRange,
        buf: &mut oio::WritableBuf,
    ) -> Result<usize> {
        let req = Request::get(url);

        // set body
        let req = req
            .header(header::RANGE, range.to_header())
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => body.read(buf).await,
            StatusCode::RANGE_NOT_SATISFIABLE => {
                body.consume().await?;
                Ok(0)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn ensure_dir_exists(&self, path: &str) -> Result<()> {
        let path = build_abs_path(&self.root, path);

        let paths = path.split('/').collect::<Vec<&str>>();

        for i in 0..paths.len() - 1 {
            let path = paths[..i + 1].join("/");
            self.create_folder_if_not_exists(&path).await?;
        }
        Ok(())
    }

    pub async fn create_folder_if_not_exists(&self, path: &str) -> Result<()> {
        let url = format!(
            "{}/createfolderifnotexists?path=/{}&username={}&password={}",
            self.endpoint,
            percent_encode_path(path),
            self.username,
            self.password
        );

        let req = Request::post(url);

        // set body
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: PcloudError = body.to_json().await?;
                let result = resp.result;
                if result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, &format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn rename_file(&self, from: &str, to: &str) -> Result<()> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: PcloudError = body.to_json().await?;
                let result = resp.result;
                if result == 2009 || result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, &format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }

                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn rename_folder(&self, from: &str, to: &str) -> Result<()> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: PcloudError = body.to_json().await?;
                let result = resp.result;
                if result == 2009 || result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, &format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }

                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn delete_folder(&self, path: &str) -> Result<()> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: PcloudError = body.to_json().await?;
                let result = resp.result;

                // pCloud returns 2005 or 2009 if the file or folder is not found
                if result != 0 && result != 2005 && result != 2009 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }

                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn delete_file(&self, path: &str) -> Result<()> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: PcloudError = body.to_json().await?;
                let result = resp.result;

                // pCloud returns 2005 or 2009 if the file or folder is not found
                if result != 0 && result != 2005 && result != 2009 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }

                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn copy_file(&self, from: &str, to: &str) -> Result<()> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: PcloudError = body.to_json().await?;
                let result = resp.result;
                if result == 2009 || result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, &format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }

                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn copy_folder(&self, from: &str, to: &str) -> Result<()> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: PcloudError = body.to_json().await?;
                let result = resp.result;
                if result == 2009 || result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, &format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }

                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn stat(&self, path: &str) -> Result<Metadata> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: StatResponse = body.to_json().await?;
                let result = resp.result;
                if result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, &format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }

                if let Some(md) = resp.metadata {
                    return parse_stat_metadata(md);
                }

                Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")))
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn upload_file(&self, path: &str, bs: Bytes) -> Result<()> {
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
        let req = req
            .body(RequestBody::Bytes(bs))
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let resp: PcloudError = body.to_json().await?;
                let result = resp.result;

                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, &format!("{resp:?}")));
                }

                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn list_folder(&self, path: &str) -> Result<ListFolderResponse> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let output = body.to_json().await?;
                Ok(output)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
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
    pub name: String,
    pub modified: String,
    pub isfolder: bool,
    pub size: Option<u64>,
    pub contenttype: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListFolderResponse {
    pub result: u64,
    pub metadata: Option<ListMetadata>,
}

#[derive(Debug, Deserialize)]
pub struct ListMetadata {
    pub name: String,
    pub path: String,
    pub modified: String,
    pub isfolder: bool,
    pub size: Option<u64>,
    pub contenttype: Option<String>,
    pub contents: Option<Vec<ListMetadata>>,
}
