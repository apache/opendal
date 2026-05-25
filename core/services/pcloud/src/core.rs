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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

use bytes::Buf;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use opendal_core::raw::*;
use opendal_core::*;
use serde::Deserialize;
use sha1::Digest;
use sha1::Sha1;

use super::error::PcloudError;
use super::error::parse_error;

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
    file_ids: RwLock<HashMap<String, u64>>,
    download_links: RwLock<HashMap<String, CachedDownloadLink>>,
}

#[derive(Debug, Clone)]
struct CachedDownloadLink {
    url: String,
    expires_at: Timestamp,
}

impl Debug for PcloudCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PcloudCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

impl PcloudCore {
    pub fn new(
        info: Arc<AccessorInfo>,
        root: String,
        endpoint: String,
        username: String,
        password: String,
    ) -> Self {
        Self {
            info,
            root,
            endpoint,
            username,
            password,
            file_ids: RwLock::new(HashMap::new()),
            download_links: RwLock::new(HashMap::new()),
        }
    }

    #[inline]
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        self.info.http_client().send(req).await
    }

    fn normalize_cached_path(&self, path: &str) -> String {
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            build_rooted_abs_path(&self.root, path)
        };

        if path == "/" {
            path
        } else {
            path.trim_end_matches('/').to_string()
        }
    }

    fn cached_file_id(&self, path: &str) -> Option<u64> {
        let path = self.normalize_cached_path(path);
        self.file_ids
            .read()
            .expect("file id cache lock poisoned")
            .get(&path)
            .copied()
    }

    fn cached_download_link(&self, path: &str) -> Option<String> {
        let path = self.normalize_cached_path(path);
        let now = Timestamp::now();
        let mut links = self
            .download_links
            .write()
            .expect("download link cache lock poisoned");

        match links.get(&path) {
            Some(link) if link.expires_at > now => Some(link.url.clone()),
            Some(_) => {
                links.remove(&path);
                None
            }
            None => None,
        }
    }

    fn cache_download_link(&self, path: &str, url: String, expires: &str) -> Result<()> {
        let path = self.normalize_cached_path(path);
        let expires_at = Timestamp::parse_rfc2822(expires)?;

        self.download_links
            .write()
            .expect("download link cache lock poisoned")
            .insert(path, CachedDownloadLink { url, expires_at });

        Ok(())
    }

    pub fn cache_file_id(&self, path: &str, file_id: u64) {
        let path = self.normalize_cached_path(path);
        self.file_ids
            .write()
            .expect("file id cache lock poisoned")
            .insert(path, file_id);
    }

    pub fn invalidate_path_cache(&self, path: &str) {
        let path = self.normalize_cached_path(path);

        self.file_ids
            .write()
            .expect("file id cache lock poisoned")
            .remove(&path);
        self.download_links
            .write()
            .expect("download link cache lock poisoned")
            .remove(&path);
    }

    pub fn invalidate_path_prefix_cache(&self, path: &str) {
        let prefix = format!(
            "{}/",
            self.normalize_cached_path(path).trim_end_matches('/')
        );

        self.file_ids
            .write()
            .expect("file id cache lock poisoned")
            .retain(|entry_path, _| !entry_path.starts_with(&prefix));
        self.download_links
            .write()
            .expect("download link cache lock poisoned")
            .retain(|entry_path, _| !entry_path.starts_with(&prefix));
    }

    async fn build_url(&self, method: &str, query: String) -> Result<String> {
        let auth_query = self.digest_auth_query().await?;

        if query.is_empty() {
            Ok(format!("{}/{method}?{auth_query}", self.endpoint))
        } else {
            Ok(format!("{}/{method}?{query}&{auth_query}", self.endpoint))
        }
    }

    async fn digest_auth_query(&self) -> Result<String> {
        let digest = self.get_digest().await?;
        let passworddigest = build_passworddigest(&self.username, &self.password, &digest);

        Ok(format!(
            "username={}&digest={}&passworddigest={}",
            percent_encode_path(&self.username),
            percent_encode_path(&digest),
            percent_encode_path(&passworddigest)
        ))
    }

    async fn get_digest(&self) -> Result<String> {
        let req = Request::get(format!("{}/getdigest", self.endpoint))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        match resp.status() {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: GetDigestResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                if resp.result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                }

                let debug = format!("{resp:?}");
                match resp.digest {
                    Some(digest) => Ok(digest),
                    None => Err(Error::new(ErrorKind::Unexpected, debug)),
                }
            }
            _ => Err(parse_error(resp)),
        }
    }
}

impl PcloudCore {
    async fn get_file_link_by_query(&self, path: &str, query: String) -> Result<String> {
        let url = self.build_url("getfilelink", query).await?;

        let req = Request::get(url);

        // set body
        let req = req
            .extension(Operation::Read)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.send(req).await?;

        let status = resp.status();
        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: GetFileLinkResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                let result = resp.result;
                if result == 2009 || result == 2010 || result == 2055 || result == 2002 {
                    return Err(Error::new(ErrorKind::NotFound, format!("{resp:?}")));
                }
                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                }

                if let Some(hosts) = resp.hosts {
                    if let Some(link_path) = resp.path {
                        if !hosts.is_empty() {
                            let url = format!("https://{}{}", hosts[0], link_path);
                            if let Some(expires) = resp.expires.as_deref() {
                                self.cache_download_link(path, url.clone(), expires)?;
                            }
                            return Ok(url);
                        }
                    }
                }
                Err(Error::new(ErrorKind::Unexpected, "hosts is empty"))
            }
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn get_file_link(&self, path: &str) -> Result<String> {
        let path = self.normalize_cached_path(path);

        if let Some(url) = self.cached_download_link(&path) {
            return Ok(url);
        }

        if let Some(file_id) = self.cached_file_id(&path) {
            match self
                .get_file_link_by_query(&path, format!("fileid={file_id}"))
                .await
            {
                Ok(url) => return Ok(url),
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    self.invalidate_path_cache(&path);
                }
                Err(err) => return Err(err),
            }
        }

        self.get_file_link_by_query(
            &path,
            format!(
                "path=/{}",
                percent_encode_path(path.trim_start_matches('/'))
            ),
        )
        .await
    }

    pub async fn download(&self, url: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let req = Request::get(url);

        // set body
        let req = req
            .header(header::RANGE, range.to_header())
            .extension(Operation::Read)
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
        let url = self
            .build_url(
                "createfolderifnotexists",
                format!("path=/{}", percent_encode_path(path)),
            )
            .await?;

        let req = Request::post(url);

        // set body
        let req = req
            .extension(Operation::CreateDir)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn rename_file(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let url = self
            .build_url(
                "renamefile",
                format!(
                    "path=/{}&topath=/{}",
                    percent_encode_path(&from),
                    percent_encode_path(&to)
                ),
            )
            .await?;

        let req = Request::post(url);

        // set body
        let req = req
            .extension(Operation::Rename)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn rename_folder(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);
        let url = self
            .build_url(
                "renamefolder",
                format!(
                    "path=/{}&topath=/{}",
                    percent_encode_path(&from),
                    percent_encode_path(&to)
                ),
            )
            .await?;

        let req = Request::post(url);

        // set body
        let req = req
            .extension(Operation::Rename)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn delete_folder(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = self
            .build_url(
                "deletefolder",
                format!("path=/{}", percent_encode_path(&path)),
            )
            .await?;

        let req = Request::post(url);

        // set body
        let req = req
            .extension(Operation::Delete)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn delete_file(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let url = self
            .build_url(
                "deletefile",
                format!("path=/{}", percent_encode_path(&path)),
            )
            .await?;

        let req = Request::post(url);

        // set body
        let req = req
            .extension(Operation::Delete)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn copy_file(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let url = self
            .build_url(
                "copyfile",
                format!(
                    "path=/{}&topath=/{}",
                    percent_encode_path(&from),
                    percent_encode_path(&to)
                ),
            )
            .await?;

        let req = Request::post(url);

        // set body
        let req = req
            .extension(Operation::Copy)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn copy_folder(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_abs_path(&self.root, to);

        let url = self
            .build_url(
                "copyfolder",
                format!(
                    "path=/{}&topath=/{}",
                    percent_encode_path(&from),
                    percent_encode_path(&to)
                ),
            )
            .await?;

        let req = Request::post(url);

        // set body
        let req = req
            .extension(Operation::Copy)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn stat(&self, path: &str) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let path = path.trim_end_matches('/');

        let url = self
            .build_url("stat", format!("path=/{}", percent_encode_path(path)))
            .await?;

        let req = Request::post(url);

        // set body
        let req = req
            .extension(Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn upload_file(&self, path: &str, bs: Buffer) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let (name, path) = (get_basename(&path), get_parent(&path).trim_end_matches('/'));

        let url = self
            .build_url(
                "uploadfile",
                format!(
                    "path=/{}&filename={}",
                    percent_encode_path(path),
                    percent_encode_path(name)
                ),
            )
            .await?;

        let req = Request::put(url);

        // set body
        let req = req
            .extension(Operation::Write)
            .body(bs)
            .map_err(new_request_build_error)?;

        self.send(req).await
    }

    pub async fn list_folder(&self, path: &str, recursive: bool) -> Result<Response<Buffer>> {
        let path = build_abs_path(&self.root, path);

        let path = normalize_root(&path);

        let path = path.trim_end_matches('/');

        let mut query = format!("path={}", percent_encode_path(path));
        if recursive {
            query.push_str("&recursive=1");
        }

        let url = self.build_url("listfolder", query).await?;

        let req = Request::get(url);

        // set body
        let req = req
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

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

    md.set_last_modified(Timestamp::parse_rfc2822(&content.modified)?);

    Ok(md)
}

pub(super) fn parse_list_metadata(content: &ListMetadata) -> Result<Metadata> {
    let mut md = if content.isfolder {
        Metadata::new(EntryMode::DIR)
    } else {
        Metadata::new(EntryMode::FILE)
    };

    if let Some(size) = content.size {
        md.set_content_length(size);
    }

    md.set_last_modified(Timestamp::parse_rfc2822(&content.modified)?);

    Ok(md)
}

fn build_passworddigest(username: &str, password: &str, digest: &str) -> String {
    let username_hash = sha1_hex(username.to_lowercase().as_bytes());
    sha1_hex(format!("{password}{username_hash}{digest}").as_bytes())
}

fn sha1_hex(input: &[u8]) -> String {
    format!("{:x}", Sha1::digest(input))
}

#[derive(Debug, Deserialize)]
pub struct GetDigestResponse {
    pub result: u64,
    pub digest: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetFileLinkResponse {
    pub result: u64,
    pub path: Option<String>,
    pub expires: Option<String>,
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
    pub fileid: Option<u64>,
    pub size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ListFolderResponse {
    pub result: u64,
    pub metadata: Option<ListMetadata>,
}

#[derive(Debug, Deserialize)]
pub struct ListMetadata {
    pub path: Option<String>,
    pub name: String,
    pub modified: String,
    pub isfolder: bool,
    pub fileid: Option<u64>,
    pub size: Option<u64>,
    pub contents: Option<Vec<ListMetadata>>,
}

#[cfg(test)]
mod tests {
    use super::ListMetadata;
    use super::build_passworddigest;
    use super::parse_list_metadata;

    #[test]
    fn build_passworddigest_lowercases_username() {
        assert_eq!(
            build_passworddigest("Alice@example.com", "s3cr3t", "abc123"),
            "994e8926e4d573e614f3f56de6449ad81288cc87"
        );
    }

    #[test]
    fn parse_list_metadata_preserves_file_size() {
        let md = parse_list_metadata(&ListMetadata {
            path: Some("/repo/data/00/file".to_string()),
            name: "file".to_string(),
            modified: "Mon, 18 May 2026 18:00:17 +0000".to_string(),
            isfolder: false,
            fileid: Some(42),
            size: Some(123),
            contents: None,
        })
        .expect("metadata should parse");

        assert_eq!(md.content_length(), 123);
        assert!(md.is_file());
    }
}
