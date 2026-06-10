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

use bytes::Buf;
use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use mea::mutex::Mutex;
use serde::Deserialize;
use serde_json::json;

use super::error::parse_error;
use super::path_index::GdrivePathIndex;
use opendal_core::raw::*;
use opendal_core::*;

pub struct GdriveCore {
    pub info: Arc<AccessorInfo>,

    pub root: String,

    pub signer: Arc<Mutex<GdriveSigner>>,

    /// Service-local path index for resolving path -> file id mappings.
    pub path_index: GdrivePathIndex<GdrivePathQuery>,

    /// Keep a short-lived local view of recent mutations so list and stat
    /// operations stay read-after-write consistent within the current operator
    /// session.
    pub recent_entries: Mutex<GdriveRecentState>,
}

impl Debug for GdriveCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GdriveCore")
            .field("root", &self.root)
            .finish()
    }
}

impl GdriveCore {
    pub async fn cache_file_id(&self, path: &str, file_id: &str) {
        self.path_index.upsert_file(path, file_id).await;
    }

    pub async fn cache_dir_id(&self, path: &str, file_id: &str) {
        self.path_index.upsert_dir(path, file_id).await;
    }

    pub async fn invalidate_file_id(&self, path: &str) {
        self.path_index.invalidate_file(path).await;
    }

    pub async fn invalidate_dir_id(&self, path: &str) {
        self.path_index.invalidate_dir(path).await;
    }

    pub async fn refresh_path(&self, path: &str) {
        self.path_index.refresh_path(path).await;
    }

    pub async fn refresh_dir_path(&self, path: &str) {
        self.invalidate_dir_id(path).await;
        self.refresh_path(path).await;
    }

    pub async fn resolve_path(&self, path: &str) -> Result<Option<String>> {
        self.path_index.get(path).await
    }

    pub async fn resolve_path_after_refresh(&self, path: &str) -> Result<Option<String>> {
        self.refresh_path(path).await;
        self.resolve_path(path).await
    }

    pub async fn ensure_dir(&self, path: &str) -> Result<String> {
        self.path_index.ensure_dir(path).await
    }

    pub async fn trash_path_if_exists(&self, path: &str) -> Result<()> {
        let mut target_id = match self.resolve_path(path).await? {
            Some(id) => Some(id),
            None => self.resolve_path_after_refresh(path).await?,
        };

        if let Some(id) = target_id.take() {
            let mut resp = self.gdrive_trash(&id).await?;
            if resp.status() == StatusCode::NOT_FOUND {
                self.refresh_path(path).await;
                target_id = self.resolve_path(path).await?;
                if let Some(id) = target_id {
                    resp = self.gdrive_trash(&id).await?;
                } else {
                    return Ok(());
                }
            }

            if resp.status() == StatusCode::NOT_FOUND {
                return Ok(());
            }
            if resp.status() != StatusCode::OK {
                return Err(parse_error(resp));
            }

            self.invalidate_file_id(path).await;
            self.invalidate_dir_id(path).await;
        }

        Ok(())
    }

    pub async fn record_recent_upsert(&self, path: &str, metadata: Metadata) {
        let mut recent_entries = self.recent_entries.lock().await;
        prune_recent_entries(&mut recent_entries);

        let mode = metadata.mode();
        let path = canonical_recent_path(path, mode);
        let expires_at = Timestamp::now() + Duration::from_secs(15);
        insert_recent_entry(
            &mut recent_entries.entries,
            &path,
            mode,
            GdriveRecentEntry {
                metadata: Some(metadata),
                expires_at,
            },
        );
        revive_recent_parent_dirs(&mut recent_entries, &path, expires_at);
    }

    pub async fn record_recent_delete(&self, path: &str, mode: EntryMode) {
        let mut recent_entries = self.recent_entries.lock().await;
        prune_recent_entries(&mut recent_entries);

        let path = canonical_recent_path(path, mode);
        let expires_at = Timestamp::now() + Duration::from_secs(15);
        insert_recent_entry(
            &mut recent_entries.entries,
            &path,
            mode,
            GdriveRecentEntry {
                metadata: None,
                expires_at,
            },
        );
        if mode.is_dir() {
            recent_entries.tombstones.insert(path, expires_at);
        }
    }

    pub async fn recent_entry_for_path(&self, path: &str) -> GdriveRecentPathState {
        let mut recent_entries = self.recent_entries.lock().await;
        prune_recent_entries(&mut recent_entries);

        let recent_entry = lookup_recent_entry(&recent_entries.entries, path);
        let tombstone = lookup_recent_tombstone(&recent_entries.tombstones, path);

        match (recent_entry, tombstone) {
            (Some(entry), Some(tombstone_expires_at))
                if tombstone_expires_at > entry.expires_at =>
            {
                GdriveRecentPathState::Deleted
            }
            (Some(entry), _) => match entry.metadata.clone() {
                Some(metadata) => GdriveRecentPathState::Present(Box::new(metadata)),
                None => GdriveRecentPathState::Deleted,
            },
            (None, Some(_)) => GdriveRecentPathState::Deleted,
            (None, None) => GdriveRecentPathState::Missing,
        }
    }

    pub async fn recent_entries_for_list(
        &self,
        scope_path: &str,
        recursive: bool,
    ) -> Vec<(String, Metadata)> {
        let mut recent_entries = self.recent_entries.lock().await;
        prune_recent_entries(&mut recent_entries);

        let mut entries = recent_entries
            .entries
            .iter()
            .filter_map(|(path, entry)| {
                let metadata = entry.metadata.clone()?;
                if metadata.mode().is_dir() && path != &normalize_dir_path(path) {
                    return None;
                }
                if let Some(tombstone_expires_at) =
                    lookup_recent_tombstone(&recent_entries.tombstones, path)
                {
                    if tombstone_expires_at > entry.expires_at {
                        return None;
                    }
                }
                if let Some(latest_entry) = lookup_recent_entry(&recent_entries.entries, path) {
                    if latest_entry.expires_at > entry.expires_at {
                        return None;
                    }
                }
                if recent_entry_in_scope(scope_path, path, metadata.mode(), recursive) {
                    Some((path.clone(), metadata))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }

    pub async fn gdrive_stat_by_id(&self, file_id: &str) -> Result<Response<Buffer>> {
        // The file metadata in the Google Drive API is very complex.
        // For now, we only need the file id, name, mime type and modified time.
        let mut req = Request::get(format!(
            "https://www.googleapis.com/drive/v3/files/{file_id}?fields=id,name,mimeType,size,modifiedTime"
        ))
        .extension(Operation::Stat)
        .body(Buffer::new())
        .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    pub async fn gdrive_get(&self, path: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let path = build_abs_path(&self.root, path);
        match self.recent_entry_for_path(&path).await {
            GdriveRecentPathState::Deleted => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("path not found: {path}"),
                ));
            }
            GdriveRecentPathState::Present(_) | GdriveRecentPathState::Missing => {}
        }
        let path_id = match self.resolve_path(&path).await? {
            Some(id) => id,
            None => match self.resolve_path_after_refresh(&path).await? {
                Some(id) => id,
                None => {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        format!("path not found: {path}"),
                    ));
                }
            },
        };

        let url: String = format!("https://www.googleapis.com/drive/v3/files/{path_id}?alt=media");

        let mut req = Request::get(&url)
            .extension(Operation::Read)
            .header(header::RANGE, range.to_header())
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        self.info.http_client().fetch(req).await
    }

    pub async fn gdrive_list(
        &self,
        file_id: &str,
        page_size: i32,
        next_page_token: &str,
    ) -> Result<Response<Buffer>> {
        let q = format!("'{file_id}' in parents and trashed = false");
        let url = "https://www.googleapis.com/drive/v3/files";
        let mut url = QueryPairsWriter::new(url);
        url = url.push("pageSize", &page_size.to_string());
        url = url.push("q", &percent_encode_path(&q));
        url = url.push(
            "fields",
            "nextPageToken,files(id,name,mimeType,size,modifiedTime,parents)",
        );
        if !next_page_token.is_empty() {
            url = url.push("pageToken", next_page_token);
        };

        let mut req = Request::get(url.finish())
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    /// List multiple directories in a single API call using OR query.
    /// This is much more efficient than listing directories one by one.
    /// Google Drive API supports up to ~50 parent IDs in a single query.
    pub async fn gdrive_list_batch(
        &self,
        file_ids: &[String],
        page_size: i32,
        next_page_token: &str,
    ) -> Result<Response<Buffer>> {
        if file_ids.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "file_ids cannot be empty",
            ));
        }

        // Build OR query: ('id1' in parents or 'id2' in parents or ...)
        let parents_query = file_ids
            .iter()
            .map(|id| format!("'{}' in parents", id))
            .collect::<Vec<_>>()
            .join(" or ");
        let q = format!("({}) and trashed = false", parents_query);

        let url = "https://www.googleapis.com/drive/v3/files";
        let mut url = QueryPairsWriter::new(url);
        url = url.push("pageSize", &page_size.to_string());
        url = url.push("q", &percent_encode_path(&q));
        // Include 'parents' field to know which directory each file belongs to
        url = url.push(
            "fields",
            "nextPageToken,files(id,name,mimeType,size,modifiedTime,parents)",
        );
        if !next_page_token.is_empty() {
            url = url.push("pageToken", next_page_token);
        };

        let mut req = Request::get(url.finish())
            .extension(Operation::List)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    // Update with content and metadata
    pub async fn gdrive_patch_metadata_request(
        &self,
        source: &str,
        target: &str,
    ) -> Result<Response<Buffer>> {
        let source_file_id = match self.resolve_path(source).await? {
            Some(id) => id,
            None => self
                .resolve_path_after_refresh(source)
                .await?
                .ok_or(Error::new(
                    ErrorKind::NotFound,
                    format!("source path not found: {source}"),
                ))?,
        };
        let source_parent = get_parent(source);
        let source_parent_id = match self.resolve_path(source_parent).await? {
            Some(id) => id,
            None => self
                .resolve_path_after_refresh(source_parent)
                .await?
                .expect("old parent must exist"),
        };

        let target_parent_id = self.path_index.ensure_dir(get_parent(target)).await?;
        let target_file_name = get_basename(target);

        let metadata = &json!({
            "name": target_file_name,
            "removeParents": [source_parent_id],
            "addParents": [target_parent_id],
        });

        let url = format!("https://www.googleapis.com/drive/v3/files/{source_file_id}");
        let mut req = Request::patch(url)
            .extension(Operation::Rename)
            .body(Buffer::from(Bytes::from(metadata.to_string())))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    pub async fn gdrive_trash(&self, file_id: &str) -> Result<Response<Buffer>> {
        let url = format!("https://www.googleapis.com/drive/v3/files/{file_id}");

        let body = serde_json::to_vec(&json!({
            "trashed": true
        }))
        .map_err(new_json_serialize_error)?;

        let mut req = Request::patch(&url)
            .extension(Operation::Delete)
            .body(Buffer::from(Bytes::from(body)))
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    /// Create a file with the content.
    pub async fn gdrive_upload_simple_request(
        &self,
        path: &str,
        size: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let parent = self.path_index.ensure_dir(get_parent(path)).await?;

        let url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart";

        let file_name = get_basename(path);

        let metadata = serde_json::to_vec(&json!({
            "name": file_name,
            "parents": [parent],
        }))
        .map_err(new_json_serialize_error)?;

        let req = Request::post(url)
            .header("X-Upload-Content-Length", size)
            .extension(Operation::Write);

        let multipart = Multipart::new()
            .part(
                FormDataPart::new("metadata")
                    .header(
                        header::CONTENT_TYPE,
                        "application/json; charset=UTF-8".parse().unwrap(),
                    )
                    .content(metadata),
            )
            .part(
                FormDataPart::new("file")
                    .header(
                        header::CONTENT_TYPE,
                        "application/octet-stream".parse().unwrap(),
                    )
                    .content(body),
            );

        let mut req = multipart.apply(req)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    /// Overwrite the file with the content.
    ///
    /// # Notes
    ///
    /// - The file id is required. Do not use this method to create a file.
    pub async fn gdrive_upload_overwrite_simple_request(
        &self,
        file_id: &str,
        size: u64,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let url =
            format!("https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media");

        let mut req = Request::patch(url)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, size)
            .header("X-Upload-Content-Length", size)
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let mut signer = self.signer.lock().await;
        signer.sign(req).await
    }

    pub async fn gdrive_copy(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);

        let from_file_id = match self.resolve_path(&from).await? {
            Some(id) => id,
            None => match self.resolve_path_after_refresh(&from).await? {
                Some(id) => id,
                None => {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        "the file to copy does not exist",
                    ));
                }
            },
        };

        let to_name = get_basename(to);
        let to_path = build_abs_path(&self.root, to);
        let to_parent_id = self.path_index.ensure_dir(get_parent(&to_path)).await?;

        self.trash_path_if_exists(&to_path).await?;

        let url = format!("https://www.googleapis.com/drive/v3/files/{from_file_id}/copy");

        let request_body = &json!({
            "name": to_name,
            "parents": [to_parent_id],
        });
        let body = Buffer::from(Bytes::from(request_body.to_string()));

        let mut req = Request::post(&url)
            .extension(Operation::Copy)
            .body(body)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        self.info.http_client().send(req).await
    }
}

#[derive(Clone)]
pub struct GdriveRecentEntry {
    metadata: Option<Metadata>,
    expires_at: Timestamp,
}

#[derive(Default)]
pub struct GdriveRecentState {
    entries: HashMap<String, GdriveRecentEntry>,
    tombstones: HashMap<String, Timestamp>,
}

#[derive(Clone, Debug)]
pub enum GdriveRecentPathState {
    Missing,
    Deleted,
    Present(Box<Metadata>),
}

pub(crate) fn normalize_dir_path(path: &str) -> String {
    if path.is_empty() || path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

fn canonical_recent_path(path: &str, mode: EntryMode) -> String {
    if mode.is_dir() {
        normalize_dir_path(path)
    } else {
        path.to_string()
    }
}

fn prune_recent_entries(entries: &mut GdriveRecentState) {
    let now = Timestamp::now();
    entries.entries.retain(|_, entry| entry.expires_at > now);
    entries.tombstones.retain(|_, expires_at| *expires_at > now);
}

fn lookup_recent_entry<'a>(
    entries: &'a HashMap<String, GdriveRecentEntry>,
    path: &str,
) -> Option<&'a GdriveRecentEntry> {
    entries.get(path)
}

fn lookup_recent_tombstone(
    tombstones: &HashMap<String, Timestamp>,
    path: &str,
) -> Option<Timestamp> {
    let candidates = recent_path_candidates(path);
    tombstones
        .iter()
        .filter(|(tombstone, _)| {
            candidates
                .iter()
                .any(|candidate| path_under_tombstone(candidate, tombstone))
        })
        .map(|(_, expires_at)| *expires_at)
        .max()
}

fn revive_recent_parent_dirs(entries: &mut GdriveRecentState, path: &str, expires_at: Timestamp) {
    let mut parent = get_parent(path).to_string();

    while !parent.is_empty() && parent != "/" {
        if lookup_recent_tombstone(&entries.tombstones, &parent).is_some() {
            insert_recent_entry(
                &mut entries.entries,
                &parent,
                EntryMode::DIR,
                GdriveRecentEntry {
                    metadata: Some(Metadata::new(EntryMode::DIR)),
                    expires_at,
                },
            );
        }

        parent = get_parent(&parent).to_string();
    }
}

fn insert_recent_entry(
    entries: &mut HashMap<String, GdriveRecentEntry>,
    path: &str,
    mode: EntryMode,
    entry: GdriveRecentEntry,
) {
    entries.insert(path.to_string(), entry.clone());

    if mode.is_dir() {
        let alias = path.trim_end_matches('/');
        if !alias.is_empty() {
            entries.insert(alias.to_string(), entry);
        }
    }
}

fn recent_path_candidates(path: &str) -> Vec<String> {
    let mut candidates = vec![path.to_string()];
    let dir_path = normalize_dir_path(path);
    if dir_path != path {
        candidates.push(dir_path);
    }
    candidates
}

fn path_under_tombstone(path: &str, tombstone: &str) -> bool {
    tombstone.is_empty() || path == tombstone || path.starts_with(tombstone)
}

fn recent_entry_in_scope(scope_path: &str, path: &str, mode: EntryMode, recursive: bool) -> bool {
    if recursive {
        if scope_path.is_empty() {
            return true;
        }

        if scope_path.ends_with('/') {
            return path.starts_with(scope_path) && path != scope_path;
        }

        return path == canonical_recent_path(scope_path, mode) || path.starts_with(scope_path);
    }

    let parent = get_parent(path);
    if scope_path.is_empty() {
        parent == "/"
    } else {
        parent == scope_path
    }
}

#[derive(Clone)]
pub struct GdriveSigner {
    pub info: Arc<AccessorInfo>,

    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,

    pub access_token: String,
    pub expires_in: Timestamp,
}

impl GdriveSigner {
    /// Create a new signer.
    pub fn new(info: Arc<AccessorInfo>) -> Self {
        GdriveSigner {
            info,

            client_id: "".to_string(),
            client_secret: "".to_string(),
            refresh_token: "".to_string(),
            access_token: "".to_string(),
            expires_in: Timestamp::MIN,
        }
    }

    /// Sign a request.
    pub async fn sign<T>(&mut self, req: &mut Request<T>) -> Result<()> {
        if !self.access_token.is_empty() && self.expires_in > Timestamp::now() {
            let value = format!("Bearer {}", self.access_token)
                .parse()
                .expect("access token must be valid header value");

            req.headers_mut().insert(header::AUTHORIZATION, value);
            return Ok(());
        }

        let url = format!(
            "https://oauth2.googleapis.com/token?refresh_token={}&client_id={}&client_secret={}&grant_type=refresh_token",
            self.refresh_token, self.client_id, self.client_secret
        );

        {
            let req = Request::post(url)
                .header(header::CONTENT_LENGTH, 0)
                .body(Buffer::new())
                .map_err(new_request_build_error)?;

            let resp = self.info.http_client().send(req).await?;
            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let resp_body = resp.into_body();
                    let token: GdriveTokenResponse = serde_json::from_reader(resp_body.reader())
                        .map_err(new_json_deserialize_error)?;
                    self.access_token.clone_from(&token.access_token);
                    self.expires_in = Timestamp::now() + Duration::from_secs(token.expires_in)
                        - Duration::from_secs(120);
                }
                _ => {
                    return Err(parse_error(resp));
                }
            }
        }

        let auth_header_content = format!("Bearer {}", self.access_token);
        req.headers_mut()
            .insert(header::AUTHORIZATION, auth_header_content.parse().unwrap());

        Ok(())
    }
}

pub struct GdrivePathQuery {
    pub info: Arc<AccessorInfo>,
    pub signer: Arc<Mutex<GdriveSigner>>,
}

impl GdrivePathQuery {
    pub fn new(info: Arc<AccessorInfo>, signer: Arc<Mutex<GdriveSigner>>) -> Self {
        GdrivePathQuery { info, signer }
    }
}

impl PathQuery for GdrivePathQuery {
    async fn root(&self) -> Result<String> {
        Ok("root".to_string())
    }

    async fn query(&self, parent_id: &str, name: &str) -> Result<Option<String>> {
        let mut queries = vec![
            // Make sure name has been replaced with escaped name.
            //
            // ref: <https://developers.google.com/drive/api/guides/ref-search-terms>
            format!(
                "name = '{}'",
                name.replace('\'', "\\'").trim_end_matches('/')
            ),
            format!("'{}' in parents", parent_id),
            "trashed = false".to_string(),
        ];
        if name.ends_with('/') {
            queries.push("mimeType = 'application/vnd.google-apps.folder'".to_string());
        }
        let query = queries.join(" and ");
        let order_by = "modifiedTime desc,createdTime desc";

        let url = format!(
            "https://www.googleapis.com/drive/v3/files?q={}&orderBy={}&pageSize=1",
            percent_encode_path(query.as_str()),
            percent_encode_path(order_by)
        );

        let mut req = Request::get(&url)
            .extension(Operation::Stat)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.signer.lock().await.sign(&mut req).await?;

        let resp = self.info.http_client().send(req).await?;
        let status = resp.status();

        match status {
            StatusCode::OK => {
                let body = resp.into_body();
                let meta: GdriveFileList =
                    serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;

                if let Some(f) = meta.files.first() {
                    Ok(Some(f.id.clone()))
                } else {
                    Ok(None)
                }
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn create_dir(&self, parent_id: &str, name: &str) -> Result<String> {
        let url = "https://www.googleapis.com/drive/v3/files";

        let content = serde_json::to_vec(&json!({
            "name": name.trim_end_matches('/'),
            "mimeType": "application/vnd.google-apps.folder",
            // If the parent is not provided, the folder will be created in the root folder.
            "parents": [parent_id],
        }))
        .map_err(new_json_serialize_error)?;

        let mut req = Request::post(url)
            .extension(Operation::CreateDir)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Buffer::from(Bytes::from(content)))
            .map_err(new_request_build_error)?;

        self.signer.lock().await.sign(&mut req).await?;

        let resp = self.info.http_client().send(req).await?;
        if !resp.status().is_success() {
            return Err(parse_error(resp));
        }

        let body = resp.into_body();
        let file: GdriveFile =
            serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;
        Ok(file.id)
    }
}

#[derive(Deserialize)]
pub struct GdriveTokenResponse {
    access_token: String,
    expires_in: u64,
}

/// This is the file struct returned by the Google Drive API.
/// This is a complex struct, but we only add the fields we need.
/// refer to https://developers.google.com/drive/api/reference/rest/v3/files#File
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GdriveFile {
    pub mime_type: String,
    pub id: String,
    pub name: String,
    pub size: Option<String>,
    // The modified time is not returned unless the `fields`
    // query parameter contains `modifiedTime`.
    // As we only need the modified time when we do `stat` operation,
    // if other operations(such as search) do not specify the `fields` query parameter,
    // try to access this field, it will be `None`.
    pub modified_time: Option<String>,
    // Parents are returned when using batch listing to identify which directory
    // a file belongs to. This is needed for the OR query optimization.
    #[serde(default)]
    pub parents: Vec<String>,
}

/// refer to https://developers.google.com/drive/api/reference/rest/v3/files/list
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GdriveFileList {
    pub(crate) files: Vec<GdriveFile>,
    pub(crate) next_page_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_gdrive_core() -> GdriveCore {
        let info = Arc::new(AccessorInfo::default());
        let signer = Arc::new(Mutex::new(GdriveSigner::new(info.clone())));

        GdriveCore {
            info: info.clone(),
            root: "/".to_string(),
            signer: signer.clone(),
            path_index: GdrivePathIndex::new(GdrivePathQuery::new(info, signer)),
            recent_entries: Mutex::new(GdriveRecentState::default()),
        }
    }

    #[tokio::test]
    async fn test_recent_entries_for_direct_list() {
        let core = mock_gdrive_core();

        core.record_recent_upsert(
            "parent/file.txt",
            Metadata::new(EntryMode::FILE).with_content_length(5),
        )
        .await;
        core.record_recent_upsert("parent/dir/", Metadata::new(EntryMode::DIR))
            .await;
        core.record_recent_upsert("parent/nested/file.txt", Metadata::new(EntryMode::FILE))
            .await;
        core.record_recent_delete("parent/deleted.txt", EntryMode::FILE)
            .await;

        let entries = core.recent_entries_for_list("parent/", false).await;
        let paths = entries
            .into_iter()
            .map(|(path, _)| path)
            .collect::<Vec<_>>();

        assert_eq!(
            paths,
            vec!["parent/dir/".to_string(), "parent/file.txt".to_string()]
        );
    }

    #[tokio::test]
    async fn test_recent_entries_for_recursive_prefix_list() {
        let core = mock_gdrive_core();

        core.record_recent_upsert("parent/file.txt", Metadata::new(EntryMode::FILE))
            .await;
        core.record_recent_upsert("parent/nested/file.txt", Metadata::new(EntryMode::FILE))
            .await;
        core.record_recent_upsert("prefix", Metadata::new(EntryMode::FILE))
            .await;
        core.record_recent_upsert("prefix-child", Metadata::new(EntryMode::FILE))
            .await;

        let parent_entries = core.recent_entries_for_list("parent/", true).await;
        let parent_paths = parent_entries
            .into_iter()
            .map(|(path, _)| path)
            .collect::<Vec<_>>();
        assert_eq!(
            parent_paths,
            vec![
                "parent/file.txt".to_string(),
                "parent/nested/file.txt".to_string()
            ]
        );

        let prefix_entries = core.recent_entries_for_list("prefix", true).await;
        let prefix_paths = prefix_entries
            .into_iter()
            .map(|(path, _)| path)
            .collect::<Vec<_>>();
        assert_eq!(
            prefix_paths,
            vec!["prefix".to_string(), "prefix-child".to_string()]
        );
    }

    #[tokio::test]
    async fn test_recent_entry_for_dir_alias() {
        let core = mock_gdrive_core();

        core.record_recent_upsert("parent/dir/", Metadata::new(EntryMode::DIR))
            .await;

        match core.recent_entry_for_path("parent/dir").await {
            GdriveRecentPathState::Present(_) => {}
            other => panic!("unexpected state for dir alias: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_recent_tombstone_hides_descendants_until_recreated() {
        let core = mock_gdrive_core();

        core.record_recent_upsert(
            "parent/dir/stale.txt",
            Metadata::new(EntryMode::FILE).with_content_length(1),
        )
        .await;
        core.record_recent_delete("parent/dir/", EntryMode::DIR)
            .await;

        match core.recent_entry_for_path("parent/dir").await {
            GdriveRecentPathState::Deleted => {}
            other => panic!("unexpected state for deleted dir: {other:?}"),
        }

        match core.recent_entry_for_path("parent/dir/file.txt").await {
            GdriveRecentPathState::Deleted => {}
            other => panic!("unexpected state for deleted child: {other:?}"),
        }

        match core.recent_entry_for_path("parent/dir/stale.txt").await {
            GdriveRecentPathState::Deleted => {}
            other => panic!("unexpected state for stale child: {other:?}"),
        }

        core.record_recent_upsert(
            "parent/dir/file.txt",
            Metadata::new(EntryMode::FILE).with_content_length(1),
        )
        .await;

        match core.recent_entry_for_path("parent/dir").await {
            GdriveRecentPathState::Present(_) => {}
            other => panic!("unexpected state for revived dir: {other:?}"),
        }

        match core.recent_entry_for_path("parent/dir/file.txt").await {
            GdriveRecentPathState::Present(_) => {}
            other => panic!("unexpected state for revived child: {other:?}"),
        }

        match core.recent_entry_for_path("parent/dir/stale.txt").await {
            GdriveRecentPathState::Deleted => {}
            other => panic!("unexpected state for stale sibling: {other:?}"),
        }

        let entries = core.recent_entries_for_list("parent/", true).await;
        let paths = entries
            .into_iter()
            .map(|(path, _)| path)
            .collect::<Vec<_>>();

        assert_eq!(
            paths,
            vec!["parent/dir/".to_string(), "parent/dir/file.txt".to_string()]
        );
    }
}
