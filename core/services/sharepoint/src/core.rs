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
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use bytes::Buf;
use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header;
use mea::mutex::Mutex;
use mea::once::OnceCell;

use opendal_core::raw::*;
use opendal_core::*;

use super::graph_model::*;

/// The drive and item that this operator is anchored to.
///
/// Resolved once from the configured folder URL and then reused. Anchoring on
/// the item id rather than a site-relative path means renaming the site, the
/// document library, or the folder itself does not invalidate the operator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriveAnchor {
    pub drive_id: String,
    pub item_id: String,
}

pub struct SharePointCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub root: String,
    pub folder_url: String,
    pub signer: Arc<Mutex<SharePointSigner>>,
    pub anchor: OnceCell<DriveAnchor>,
}

impl Debug for SharePointCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharePointCore")
            .field("folder_url", &self.folder_url)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

// SharePoint returns 400 when try to access a dir with the POSIX special directory entries
const SPECIAL_POSIX_ENTRIES: [&str; 3] = [".", "/", ""];

// organizes a few core module functions
impl SharePointCore {
    pub(crate) const GRAPH_URL: &'static str = "https://graph.microsoft.com/v1.0";

    /// Encode a URL into the share id accepted by the `/shares` endpoint.
    ///
    /// Microsoft specifies base64url without padding, prefixed with `u!`.
    ///
    /// Read more at https://learn.microsoft.com/en-us/graph/api/shares-get#encoding-sharing-urls
    pub(crate) fn encode_share_id(url: &str) -> String {
        format!("u!{}", BASE64_URL_SAFE_NO_PAD.encode(url.as_bytes()))
    }

    /// Resolve the configured folder URL into a drive id and item id.
    ///
    /// `/shares` accepts any SharePoint URL and hands back the `driveItem` it
    /// points at. Using it avoids having to guess where the site path ends and
    /// the document library path begins, which is not decidable from the URL
    /// alone once subsites are involved.
    pub(crate) async fn anchor(&self, ctx: &OperationContext) -> Result<&DriveAnchor> {
        self.anchor
            .get_or_try_init(|| async { self.resolve_anchor(ctx).await })
            .await
    }

    async fn resolve_anchor(&self, ctx: &OperationContext) -> Result<DriveAnchor> {
        let url = format!(
            "{}/shares/{}/driveItem?{}",
            Self::GRAPH_URL,
            Self::encode_share_id(&self.folder_url),
            ANCHOR_SELECT_PARAM
        );

        let mut request = Request::get(&url)
            .extension(Operation::Stat)
            .extension(ServiceOperation("ResolveShare"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        let response = ctx.http_transport().send(request).await?;
        if !response.status().is_success() {
            return Err(parse_error(response)
                .with_context("folder_url", self.folder_url.clone())
                .with_operation("resolve_anchor"));
        }

        let item: SharePointAnchorItem = serde_json::from_reader(response.into_body().reader())
            .map_err(new_json_deserialize_error)?;

        Ok(DriveAnchor {
            drive_id: item.parent_reference.drive_id,
            item_id: item.id,
        })
    }

    /// The Graph URL of the anchor item, which every path is addressed relative to.
    pub(crate) async fn base_url(&self, ctx: &OperationContext) -> Result<String> {
        let anchor = self.anchor(ctx).await?;
        Ok(format!(
            "{}/drives/{}/items/{}",
            Self::GRAPH_URL,
            anchor.drive_id,
            anchor.item_id
        ))
    }

    /// Get a URL to a SharePoint item, relative to the anchor.
    ///
    /// Graph supports path addressing relative to an arbitrary item id, so the
    /// anchor takes the role that `/drive/root` plays for a personal OneDrive.
    ///
    /// Read more at https://learn.microsoft.com/en-us/graph/onedrive-addressing-driveitems
    ///
    /// Only the opening `:` is emitted. Callers append the closing `:` together
    /// with whatever action segment they need.
    pub(crate) fn sharepoint_item_url(
        &self,
        base_url: &str,
        path: &str,
        build_absolute_path: bool,
    ) -> String {
        if self.root == "/" && SPECIAL_POSIX_ENTRIES.contains(&path) {
            base_url.to_string()
        } else {
            // SharePoint returns 400 when try to access a folder with a ending slash
            let absolute_path = if build_absolute_path {
                let rooted_path = build_rooted_abs_path(&self.root, path);
                rooted_path
                    .strip_suffix('/')
                    .unwrap_or(rooted_path.as_str())
                    .to_string()
            } else {
                path.to_string()
            };
            format!("{}:{}", base_url, percent_encode_path(&absolute_path))
        }
    }

    /// Send a simplest stat request about a particular path
    ///
    /// See also: [`sharepoint_stat()`].
    pub(crate) async fn sharepoint_get_stat_plain(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let base_url = self.base_url(ctx).await?;
        let url: String = format!(
            "{}?{}",
            self.sharepoint_item_url(&base_url, path, true),
            GENERAL_SELECT_PARAM
        );
        let request = Request::get(&url);

        let mut request = request
            .extension(Operation::Stat)
            .extension(ServiceOperation("GetItem"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        ctx.http_transport().send(request).await
    }

    /// Create a directory at path if not exist, return the metadata about the folder
    ///
    /// When the folder exist, this function works exactly the same as [`sharepoint_get_stat_plain()`].
    ///
    /// * `path` - a relative folder path
    pub(crate) async fn ensure_directory(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<SharePointItem> {
        let response = self.sharepoint_get_stat_plain(ctx, path).await?;
        let item: SharePointItem = match response.status() {
            StatusCode::OK => {
                let bytes = response.into_body();
                serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?
            }
            StatusCode::NOT_FOUND => {
                // We must create directory for the destination
                let response = self.sharepoint_create_dir(ctx, path).await?;
                match response.status() {
                    StatusCode::CREATED | StatusCode::OK => {
                        let bytes = response.into_body();
                        serde_json::from_reader(bytes.reader())
                            .map_err(new_json_deserialize_error)?
                    }
                    _ => return Err(parse_error(response)),
                }
            }
            _ => return Err(parse_error(response)),
        };

        Ok(item)
    }

    pub(crate) async fn sign<T>(
        &self,
        ctx: &OperationContext,
        request: &mut Request<T>,
    ) -> Result<()> {
        let mut signer = self.signer.lock().await;
        signer.sign(ctx, request).await
    }
}

// SharePoint copy action is asynchronous. We query an endpoint and wait 1 second.
// This is the maximum attempts we will wait.
const MAX_MONITOR_ATTEMPT: i32 = 3600;
const MONITOR_WAIT_SECOND: u64 = 1;

// Graph API parameters allows using with a parameter of:
//
// - ID
// - file path
//
// `services-sharepoint` addresses items by path relative to the anchor item.
// Read more at https://learn.microsoft.com/en-us/graph/onedrive-addressing-driveitems
impl SharePointCore {
    /// Send a stat request about a particular path, including:
    ///
    /// - Get stat object only if ETag not matches
    /// - whether to get the object version
    ///
    /// See also [`sharepoint_get_stat_plain()`].
    pub(crate) async fn sharepoint_stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> Result<Metadata> {
        let base_url = self.base_url(ctx).await?;
        let mut url: String = self.sharepoint_item_url(&base_url, path, true);
        if args.version().is_some() {
            url += "?$expand=versions(";
            url += VERSION_SELECT_PARAM;
            url += ")";
        }

        let mut request = Request::get(&url);
        if let Some(etag) = args.if_none_match() {
            request = request.header(header::IF_NONE_MATCH, etag);
        }

        let mut request = request
            .extension(Operation::Stat)
            .extension(ServiceOperation("GetItem"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        let response = ctx.http_transport().send(request).await?;
        if !response.status().is_success() {
            return Err(parse_error(response));
        }

        let bytes = response.into_body();
        let decoded_response: SharePointItem =
            serde_json::from_reader(bytes.reader()).map_err(new_json_deserialize_error)?;

        let entry_mode: EntryMode = match decoded_response.item_type {
            ItemType::Folder { .. } => EntryMode::DIR,
            ItemType::File { .. } => EntryMode::FILE,
        };

        let mut meta = Metadata::new(entry_mode)
            .with_etag(decoded_response.e_tag)
            .with_content_length(decoded_response.size.max(0) as u64);

        if let Some(version) = args.version() {
            for item_version in decoded_response.versions.as_deref().unwrap_or_default() {
                if item_version.id == version {
                    meta.set_version(version);
                    break; // early exit
                }
            }

            if meta.version().is_none() {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "cannot find this version of the item",
                ));
            }
        }

        let last_modified = decoded_response.last_modified_date_time;
        let date_utc_last_modified = last_modified.parse::<Timestamp>()?;
        meta.set_last_modified(date_utc_last_modified);

        Ok(meta)
    }

    /// Return versions of an item
    ///
    /// A folder has no versions.
    ///
    /// * `path` - a relative path
    pub(crate) async fn sharepoint_list_versions(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Vec<SharePointItemVersion>> {
        let base_url = self.base_url(ctx).await?;
        // don't `$select` this endpoint to get the download URL.
        let url: String = format!(
            "{}:/versions?{}",
            self.sharepoint_item_url(&base_url, path, true),
            VERSION_SELECT_PARAM
        );

        let mut request = Request::get(url)
            .extension(Operation::List)
            .extension(ServiceOperation("ListVersions"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        let response = ctx.http_transport().send(request).await?;
        if !response.status().is_success() {
            return Err(parse_error(response));
        }

        let decoded_response: GraphApiSharePointVersionsResponse =
            serde_json::from_reader(response.into_body().reader())
                .map_err(new_json_deserialize_error)?;
        Ok(decoded_response.value)
    }

    /// Build the `children` collection URL for `path`.
    ///
    /// The anchor itself is addressed as `items/{id}/children`; anything below it
    /// takes the path form `items/{id}:/{path}:/children`. Graph rejects the path
    /// form applied to the anchor, the same class of bug that
    /// "fix(services/onedrive): build correct children URL when listing root"
    /// addressed for OneDrive. Both callers must agree, so the branch lives in
    /// one place.
    pub(crate) fn sharepoint_children_url(&self, base_url: &str, path: &str) -> String {
        let item_url = self.sharepoint_item_url(base_url, path, true);
        if item_url == base_url {
            format!("{item_url}/children?{GENERAL_SELECT_PARAM}")
        } else {
            format!("{item_url}:/children?{GENERAL_SELECT_PARAM}")
        }
    }

    /// Build the request that lists the children of `path`.
    ///
    /// Kept synchronous by taking a pre-resolved `base_url`, which keeps the URL
    /// shape assertions in this module's tests free of async plumbing.
    pub(crate) fn sharepoint_list_request(
        &self,
        base_url: &str,
        path: &str,
        limit: Option<usize>,
    ) -> Result<Request<Buffer>> {
        let mut url = self.sharepoint_children_url(base_url, path);
        if let Some(limit) = limit {
            url += &format!("&$top={limit}");
        }

        Request::get(&url)
            .extension(Operation::List)
            .extension(ServiceOperation("ListChildren"))
            .body(Buffer::new())
            .map_err(new_request_build_error)
    }

    pub(crate) async fn sharepoint_list(
        &self,
        ctx: &OperationContext,
        path: &str,
        limit: Option<usize>,
    ) -> Result<Response<Buffer>> {
        let base_url = self.base_url(ctx).await?;
        let mut request = self.sharepoint_list_request(&base_url, path, limit)?;

        self.sign(ctx, &mut request).await?;

        ctx.http_transport().send(request).await
    }

    pub(crate) async fn sharepoint_get_next_list_page(
        &self,
        ctx: &OperationContext,
        url: &str,
    ) -> Result<Response<Buffer>> {
        let mut request = Request::get(url)
            .extension(Operation::List)
            .extension(ServiceOperation("ListChildren"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        ctx.http_transport().send(request).await
    }

    /// Download a file
    ///
    /// Graph handles a download in 2 steps:
    /// 1. Returns a 302 with a presigned URL. If `If-None-Match` succeed, returns 304.
    /// 2. With the presigned URL, we can send a GET:
    ///   1. When getting an item succeed with a `Range` header, we get a 206 Partial Content response.
    ///   2. When succeed, we get a 200 response.
    ///
    /// Read more at https://learn.microsoft.com/en-us/graph/api/driveitem-get-content
    pub(crate) async fn sharepoint_get_content(
        &self,
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let base_url = self.base_url(ctx).await?;
        // We can't "select" the Graph API response fields when reading because "select" shadows not found error
        let url: String = format!(
            "{}:/content",
            self.sharepoint_item_url(&base_url, path, true)
        );

        let mut request = Request::get(&url).header(header::RANGE, range.to_header());
        if let Some(etag) = args.if_none_match() {
            request = request.header(header::IF_NONE_MATCH, etag);
        }

        let mut request = request
            .extension(Operation::Read)
            .extension(ServiceOperation("DownloadContent"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        ctx.http_transport().fetch(request).await
    }

    /// Upload a file
    ///
    /// When creating a file,
    ///
    /// * Graph returns 201 if the file is new.
    /// * Graph returns 200 if successfully overwrote the file successfully.
    ///
    /// Read more at https://learn.microsoft.com/en-us/graph/api/driveitem-put-content
    ///
    /// This function is different than uploading a file with chunks.
    /// See also [`sharepoint_create_upload_session()`] and [`SharePointWriter::write_chunked`].
    pub async fn sharepoint_upload_simple(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let base_url = self.base_url(ctx).await?;
        let url = format!(
            "{}:/content?@microsoft.graph.conflictBehavior={}&{}",
            self.sharepoint_item_url(&base_url, path, true),
            REPLACE_EXISTING_ITEM_WHEN_CONFLICT,
            GENERAL_SELECT_PARAM
        );

        // The upload API documentation requires "text/plain" as the content type.
        // In practice, Graph ignores the content type,
        // but decides the type (when stating) based on the extension name.
        // Also, when the extension name is unknown, Graph sets the content type
        // as "application/octet-stream".
        // We keep the content type according to the documentation.
        let mut request = Request::put(&url)
            .header(header::CONTENT_LENGTH, body.len())
            .header(header::CONTENT_TYPE, "text/plain");

        // when creating a new file, `IF-Match` has no effect.
        // when updating a file with the `If-Match`, and if the ETag mismatched,
        // Graph will return 412 Precondition Failed
        if let Some(if_match) = args.if_match() {
            request = request.header(header::IF_MATCH, if_match);
        }

        let mut request = request
            .extension(Operation::Write)
            .extension(ServiceOperation("UploadContent"))
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        ctx.http_transport().send(request).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn sharepoint_chunked_upload(
        &self,
        ctx: &OperationContext,
        url: &str,
        args: &OpWrite,
        offset: usize,
        chunk_end: usize,
        total_len: usize,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let mut request = Request::put(url);

        let range = format!("bytes {offset}-{chunk_end}/{total_len}");
        request = request.header(header::CONTENT_RANGE, range);

        let size = chunk_end - offset + 1;
        request = request.header(header::CONTENT_LENGTH, size);

        if let Some(mime) = args.content_type() {
            request = request.header(header::CONTENT_TYPE, mime)
        }

        let request = request
            .extension(Operation::Write)
            .extension(ServiceOperation("UploadFragment"))
            .body(body)
            .map_err(new_request_build_error)?;
        // Graph documentation requires not sending the `Authorization` header:
        // the upload URL returned by `createUploadSession` is already pre-authorized.

        ctx.http_transport().send(request).await
    }

    /// Create a upload session for chunk uploads
    ///
    /// This endpoint supports `If-None-Match` but [`sharepoint_upload_simple()`] doesn't.
    ///
    /// Read more at https://learn.microsoft.com/en-us/graph/api/driveitem-createuploadsession
    pub(crate) async fn sharepoint_create_upload_session(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let base_url = self.base_url(ctx).await?;
        let parent_path = get_parent(path);
        let file_name = get_basename(path);
        let url = format!(
            "{}:/createUploadSession",
            self.sharepoint_item_url(&base_url, parent_path, true),
        );
        let mut request = Request::post(url).header(header::CONTENT_TYPE, "application/json");

        if let Some(if_match) = args.if_match() {
            request = request.header(header::IF_MATCH, if_match);
        }

        let body = SharePointUploadSessionCreationRequestBody::new(file_name.to_string());
        let body_bytes = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let body = Buffer::from(Bytes::from(body_bytes));
        let mut request = request
            .extension(Operation::Write)
            .extension(ServiceOperation("CreateUploadSession"))
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        ctx.http_transport().send(request).await
    }

    /// Create a directory
    ///
    /// When creating a folder, Graph returns a status code with 201.
    /// When using `microsoft.graph.conflictBehavior=replace` to replace a folder, Graph returns 200.
    ///
    /// * `path` - the path to the folder without the root
    pub(crate) async fn sharepoint_create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let base_url = self.base_url(ctx).await?;
        let parent_path = get_parent(path);
        let basename = get_basename(path);
        let folder_name = basename.strip_suffix('/').unwrap_or(basename);

        let url = self.sharepoint_children_url(&base_url, parent_path);

        let payload = CreateDirPayload::new(folder_name.to_string());
        let body_bytes = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;
        let body = Buffer::from(Bytes::from(body_bytes));

        let mut request = Request::post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .extension(Operation::CreateDir)
            .extension(ServiceOperation("CreateFolder"))
            .body(body)
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        ctx.http_transport().send(request).await
    }

    /// Delete a `DriveItem`
    ///
    /// This moves the items to the recycle bin.
    pub(crate) async fn sharepoint_delete(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let base_url = self.base_url(ctx).await?;
        let url = self.sharepoint_item_url(&base_url, path, true);

        let mut request = Request::delete(&url)
            .extension(Operation::Delete)
            .extension(ServiceOperation("DeleteItem"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        ctx.http_transport().send(request).await
    }

    /// Initialize a copy
    ///
    /// * `source` - the path to the source folder without the root
    /// * `destination` - the path to the destination folder without the root
    ///
    /// See also: [`wait_until_complete()`]
    pub(crate) async fn initialize_copy(
        &self,
        ctx: &OperationContext,
        source: &str,
        destination: &str,
    ) -> Result<String> {
        // we must validate if source exist
        let response = self.sharepoint_get_stat_plain(ctx, source).await?;
        if !response.status().is_success() {
            return Err(parse_error(response));
        }

        // We need to stat the destination parent folder to get a parent reference
        let destination_parent = get_parent(destination).to_string();
        let basename = get_basename(destination);

        let item = self.ensure_directory(ctx, &destination_parent).await?;
        let body = SharePointPatchRequestBody {
            parent_reference: ParentReference {
                path: "".to_string(), // irrelevant for copy
                drive_id: item.parent_reference.drive_id,
                id: item.id,
            },
            name: basename.to_string(),
        };

        // ensure the destination file or folder doesn't exist
        let response = self.sharepoint_get_stat_plain(ctx, destination).await?;
        match response.status() {
            // We must remove the file or folder because `conflictBehavior` is not
            // honored consistently by the copy endpoint.
            // Read more at https://learn.microsoft.com/en-us/graph/api/driveitem-copy
            StatusCode::OK => {
                let response = self.sharepoint_delete(ctx, destination).await?;
                match response.status() {
                    StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => {} // expected, intentionally empty
                    _ => return Err(parse_error(response)),
                }
            }
            StatusCode::NOT_FOUND => {} // expected, intentionally empty
            _ => return Err(parse_error(response)),
        }

        let base_url = self.base_url(ctx).await?;
        let url: String = format!(
            "{}:/copy",
            self.sharepoint_item_url(&base_url, source, true)
        );

        let body_bytes = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let buffer = Buffer::from(Bytes::from(body_bytes));
        let mut request = Request::post(&url)
            .header(header::CONTENT_TYPE, "application/json")
            .extension(Operation::Copy)
            .extension(ServiceOperation("CopyItem"))
            .body(buffer)
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        let response = ctx.http_transport().send(request).await?;
        match response.status() {
            StatusCode::ACCEPTED => parse_location(response.headers())?
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "SharePoint didn't return a location URL",
                    )
                })
                .map(String::from),
            _ => Err(parse_error(response)),
        }
    }

    pub(crate) async fn wait_until_complete(
        &self,
        ctx: &OperationContext,
        monitor_url: String,
    ) -> Result<()> {
        for _attempt in 0..MAX_MONITOR_ATTEMPT {
            let mut request = Request::get(monitor_url.to_string())
                .header(header::CONTENT_TYPE, "application/json")
                .extension(Operation::Copy)
                .extension(ServiceOperation("MonitorCopy"))
                .body(Buffer::new())
                .map_err(new_request_build_error)?;

            self.sign(ctx, &mut request).await?;

            let response = ctx.http_transport().send(request).await?;
            let status: SharePointMonitorStatus =
                serde_json::from_reader(response.into_body().reader())
                    .map_err(new_json_deserialize_error)?;
            if status.status == "completed" {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_secs(MONITOR_WAIT_SECOND)).await;
        }

        Err(Error::new(
            ErrorKind::Unexpected,
            "Exceed monitoring timeout",
        ))
    }

    pub(crate) async fn sharepoint_move(
        &self,
        ctx: &OperationContext,
        source: &str,
        destination: &str,
    ) -> Result<()> {
        // We must validate if the source folder exists.
        let response = self.sharepoint_get_stat_plain(ctx, source).await?;
        if !response.status().is_success() {
            return Err(Error::new(ErrorKind::NotFound, "source not found"));
        }

        // We want a parent reference about the destination's parent, or the destination folder itself.
        let destination_parent = get_parent(destination).to_string();
        let basename = get_basename(destination);

        let item = self.ensure_directory(ctx, &destination_parent).await?;
        let body = SharePointPatchRequestBody {
            parent_reference: ParentReference {
                path: "".to_string(), // irrelevant for update
                // reusing `ParentReference` for convenience. The API requires this value to be correct.
                drive_id: item.parent_reference.drive_id,
                id: item.id,
            },
            name: basename.to_string(),
        };
        let body_bytes = serde_json::to_vec(&body).map_err(new_json_serialize_error)?;
        let buffer = Buffer::from(Bytes::from(body_bytes));
        let base_url = self.base_url(ctx).await?;
        let url: String = format!(
            "{}?@microsoft.graph.conflictBehavior={}&$select=id",
            self.sharepoint_item_url(&base_url, source, true),
            REPLACE_EXISTING_ITEM_WHEN_CONFLICT
        );
        let mut request = Request::patch(&url)
            .header(header::CONTENT_TYPE, "application/json")
            .extension(Operation::Rename)
            .extension(ServiceOperation("MoveItem"))
            .body(buffer)
            .map_err(new_request_build_error)?;

        self.sign(ctx, &mut request).await?;

        let response = ctx.http_transport().send(request).await?;
        match response.status() {
            // can get etag, metadata, etc...
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(response)),
        }
    }
}

// keeps track of OAuth 2.0 tokens and refreshes the access token.
pub struct SharePointSigner {
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,

    pub access_token: String,
    pub expires_in: Timestamp,
}

/// Work and school accounts usually live in a specific tenant, so the tenant is
/// configurable. `common` remains the default for multi-tenant applications.
pub(crate) const DEFAULT_TENANT_ID: &str = "common";

/// SharePoint needs `Sites.ReadWrite.All` where a personal OneDrive only needs
/// `Files.ReadWrite`. `offline_access` is what makes a refresh token available.
const OAUTH_SCOPE: &str = "offline_access%20Sites.ReadWrite.All";

impl SharePointSigner {
    pub fn new() -> Self {
        SharePointSigner {
            tenant_id: DEFAULT_TENANT_ID.to_string(),
            client_id: "".to_string(),
            client_secret: "".to_string(),
            refresh_token: "".to_string(),
            access_token: "".to_string(),
            expires_in: Timestamp::MIN,
        }
    }

    fn token_endpoint(&self) -> String {
        format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            percent_encode_path(&self.tenant_id)
        )
    }

    async fn refresh_tokens(&mut self, ctx: &OperationContext) -> Result<()> {
        // SharePoint users must provide at least this required permission scope
        let encoded_payload = format!(
            "client_id={}&client_secret={}&scope={}&refresh_token={}&grant_type=refresh_token",
            percent_encode_path(self.client_id.as_str()),
            percent_encode_path(self.client_secret.as_str()),
            OAUTH_SCOPE,
            percent_encode_path(self.refresh_token.as_str())
        );
        let request = Request::post(self.token_endpoint())
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(encoded_payload))
            .map_err(new_request_build_error)?;

        let response = ctx.http_transport().send(request).await?;
        match response.status() {
            StatusCode::OK => {
                let resp_body = response.into_body();
                let data: GraphOAuthRefreshTokenResponseBody =
                    serde_json::from_reader(resp_body.reader())
                        .map_err(new_json_deserialize_error)?;
                self.access_token = data.access_token;
                self.refresh_token = data.refresh_token;
                self.expires_in = Timestamp::now() + Duration::from_secs(data.expires_in)
                    - Duration::from_secs(120); // assumes 2 mins graceful transmission for implementation simplicity
                Ok(())
            }
            _ => Err(parse_error(response)),
        }
    }

    /// Sign a request.
    pub async fn sign<T>(
        &mut self,
        ctx: &OperationContext,
        request: &mut Request<T>,
    ) -> Result<()> {
        if !self.access_token.is_empty() && self.expires_in > Timestamp::now() {
            let value = format!("Bearer {}", self.access_token)
                .parse()
                .expect("access_token must be valid header value");

            request.headers_mut().insert(header::AUTHORIZATION, value);
            return Ok(());
        }

        self.refresh_tokens(ctx).await?;

        let auth_header_content = format!("Bearer {}", self.access_token)
            .parse()
            .expect("Fetched access_token is invalid as a header value");

        request
            .headers_mut()
            .insert(header::AUTHORIZATION, auth_header_content);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::stream;
    use http::StatusCode;
    use opendal_core::raw::oio::List;

    use super::super::lister::SharePointLister;
    use super::*;

    const FOLDER_URL: &str =
        "https://contoso.sharepoint.com/sites/Finance/Shared%20Documents/Reports";
    const DRIVE_ID: &str = "b!driveid";
    const ITEM_ID: &str = "01ANCHOR";

    const ANCHOR_RESPONSE: &str =
        r#"{"id":"01ANCHOR","name":"Reports","parentReference":{"driveId":"b!driveid"}}"#;
    const ANCHOR_STAT_RESPONSE: &str = r#"{"id":"01ANCHOR","name":"Reports","lastModifiedDateTime":"2026-01-01T00:00:00Z","eTag":"aTag","size":0,"parentReference":{"path":"/drives/b!driveid/root:/Shared Documents","driveId":"b!driveid","id":"01PARENT"},"folder":{"childCount":1}}"#;
    const CHILDREN_RESPONSE: &str = r#"{"value":[{"id":"01CHILD","name":"test.txt","lastModifiedDateTime":"2026-01-01T00:00:00Z","eTag":"aTag","size":5,"parentReference":{"path":"/drives/b!driveid/root:/Shared Documents/Reports","driveId":"b!driveid","id":"01ANCHOR"},"file":{"mimeType":"text/plain"}}]}"#;

    fn base_url() -> String {
        format!(
            "{}/drives/{DRIVE_ID}/items/{ITEM_ID}",
            SharePointCore::GRAPH_URL
        )
    }

    #[derive(Clone)]
    struct MockHttpTransport;

    impl HttpTransport for MockHttpTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let url = req.uri().to_string();
            let base = base_url();
            let anchor_url = format!(
                "{}/shares/{}/driveItem?{}",
                SharePointCore::GRAPH_URL,
                SharePointCore::encode_share_id(FOLDER_URL),
                ANCHOR_SELECT_PARAM
            );

            let (status, body) = if url == anchor_url {
                (StatusCode::OK, ANCHOR_RESPONSE)
            } else if url == format!("{base}/children?{GENERAL_SELECT_PARAM}") {
                (StatusCode::OK, CHILDREN_RESPONSE)
            } else if url == base {
                (StatusCode::OK, ANCHOR_STAT_RESPONSE)
            } else {
                (
                    StatusCode::NOT_FOUND,
                    r#"{"error":{"code":"itemNotFound","message":"Item not found"}}"#,
                )
            };

            let data = Bytes::from_static(body.as_bytes());
            let size = data.len() as u64;
            Ok(Response::builder()
                .status(status)
                .header(header::CONTENT_LENGTH, size)
                .body(HttpBody::new(
                    stream::iter(vec![Ok(Buffer::from(data))]),
                    Some(size),
                ))
                .unwrap())
        }
    }

    fn test_ctx() -> OperationContext {
        OperationContext::new().with_http_transport(HttpTransporter::new(MockHttpTransport))
    }

    fn test_core(root: &str) -> Arc<SharePointCore> {
        let info = ServiceInfo::new("sharepoint", root, "");

        let mut signer = SharePointSigner::new();
        signer.access_token = "token".to_string();
        signer.expires_in = Timestamp::MAX;

        Arc::new(SharePointCore {
            info,
            capability: Capability::default(),
            root: root.to_string(),
            folder_url: FOLDER_URL.to_string(),
            signer: Arc::new(Mutex::new(signer)),
            anchor: OnceCell::new(),
        })
    }

    #[test]
    fn encode_share_id_uses_base64url_without_padding() {
        // "https://a" is 9 bytes, so a padded encoding would end with "===".
        let encoded = SharePointCore::encode_share_id("https://a");
        assert_eq!(encoded, "u!aHR0cHM6Ly9h");
        assert!(!encoded.contains('='));

        // base64url replaces `+` and `/` with `-` and `_`.
        let encoded = SharePointCore::encode_share_id(FOLDER_URL);
        assert!(encoded.starts_with("u!"));
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('/'));
    }

    #[test]
    fn list_request_for_root_targets_anchor_children() {
        let core = test_core("/");
        let base = base_url();
        let request = core.sharepoint_list_request(&base, "/", None).unwrap();
        assert_eq!(
            request.uri().to_string(),
            format!("{base}/children?{GENERAL_SELECT_PARAM}")
        );
    }

    #[test]
    fn list_request_for_nested_path_uses_path_addressing() {
        let core = test_core("/");
        let base = base_url();
        let request = core
            .sharepoint_list_request(&base, "foo/", Some(10))
            .unwrap();
        assert_eq!(
            request.uri().to_string(),
            format!("{base}:/foo:/children?{GENERAL_SELECT_PARAM}&$top=10")
        );
    }

    #[test]
    fn list_request_for_root_under_custom_root_uses_path_addressing() {
        let core = test_core("/base/");
        let base = base_url();
        let request = core.sharepoint_list_request(&base, "", None).unwrap();
        assert_eq!(
            request.uri().to_string(),
            format!("{base}:/base:/children?{GENERAL_SELECT_PARAM}")
        );
    }

    // `create_dir` posts to the *parent's* children collection, so it hits the
    // same anchor-vs-path branch as listing. OneDrive gets this wrong for the
    // root case, so both branches are pinned here.
    #[test]
    fn create_dir_url_for_top_level_folder_targets_anchor_children() {
        let core = test_core("/");
        let base = base_url();
        let url = core.sharepoint_children_url(&base, get_parent("foo/"));
        assert_eq!(url, format!("{base}/children?{GENERAL_SELECT_PARAM}"));
    }

    #[test]
    fn create_dir_url_for_nested_folder_uses_path_addressing() {
        let core = test_core("/");
        let base = base_url();
        let url = core.sharepoint_children_url(&base, get_parent("a/b/"));
        assert_eq!(url, format!("{base}:/a:/children?{GENERAL_SELECT_PARAM}"));
    }

    #[test]
    fn create_dir_url_under_custom_root_uses_path_addressing() {
        let core = test_core("/base/");
        let base = base_url();
        // The operator root is never the anchor, so even a top-level folder
        // resolves through the path form.
        let url = core.sharepoint_children_url(&base, get_parent("foo/"));
        assert_eq!(
            url,
            format!("{base}:/base:/children?{GENERAL_SELECT_PARAM}")
        );
    }

    #[test]
    fn item_url_percent_encodes_reserved_characters() {
        let core = test_core("/");
        let base = base_url();

        // `#` and space sit outside RFC 3986's `pchar` set and must be encoded.
        // `/` stays literal so that path addressing keeps working.
        let url = core.sharepoint_item_url(&base, "Break#Out/my file.txt", true);
        assert_eq!(url, format!("{base}:/Break%23Out/my%20file.txt"));

        // A literal `%` in a name round-trips as `%25`.
        let url = core.sharepoint_item_url(&base, "estimate%s.docx", true);
        assert_eq!(url, format!("{base}:/estimate%25s.docx"));
    }

    #[test]
    fn item_url_strips_trailing_slash_for_directories() {
        let core = test_core("/");
        let base = base_url();
        // Graph returns 400 when a folder is addressed with a trailing slash.
        let url = core.sharepoint_item_url(&base, "folder/", true);
        assert_eq!(url, format!("{base}:/folder"));
    }

    #[test]
    fn token_endpoint_uses_configured_tenant() {
        let mut signer = SharePointSigner::new();
        assert_eq!(
            signer.token_endpoint(),
            "https://login.microsoftonline.com/common/oauth2/v2.0/token"
        );

        signer.tenant_id = "5f1f11f3-a6b4-4414-aee0-215c774f80db".to_string();
        assert_eq!(
            signer.token_endpoint(),
            "https://login.microsoftonline.com/5f1f11f3-a6b4-4414-aee0-215c774f80db/oauth2/v2.0/token"
        );
    }

    #[tokio::test]
    async fn anchor_resolves_once_from_folder_url() {
        let core = test_core("/");
        let ctx = test_ctx();

        let anchor = core.anchor(&ctx).await.unwrap().clone();
        assert_eq!(anchor.drive_id, DRIVE_ID);
        assert_eq!(anchor.item_id, ITEM_ID);

        assert_eq!(core.base_url(&ctx).await.unwrap(), base_url());
    }

    #[tokio::test]
    async fn list_root_returns_entries() {
        let core = test_core("/");
        let ctx = test_ctx();
        let lister = SharePointLister::new(
            "/".to_string(),
            core,
            ctx,
            Capability::default(),
            &OpList::default(),
        );
        let mut lister = oio::PageLister::new(lister);

        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            entries.push(entry);
        }

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].mode(), EntryMode::DIR);
        assert_eq!(entries[1].path(), "test.txt");
        assert_eq!(entries[1].mode(), EntryMode::FILE);
    }
}

mod error {
    use http::Response;
    use http::StatusCode;

    use opendal_core::raw::*;
    use opendal_core::*;

    /// Parse error response into Error.
    pub(crate) fn parse_error(response: Response<Buffer>) -> Error {
        let (parts, body) = response.into_parts();
        let bs = body.to_bytes();

        let (kind, retryable) = match parts.status {
            StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
            // SharePoint does not have strong read-after-write properties, so
            // concurrent requests to create directories might result in errors.
            //
            // Running behavior tests can yield HTTP 409 Conflict because of the
            // consistency guarantee.
            //
            // Read more about `REPLACE_EXISTING_ITEM_WHEN_CONFLICT` in `graph_model.rs`.
            StatusCode::CONFLICT => (ErrorKind::AlreadyExists, true),
            StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
            StatusCode::TOO_MANY_REQUESTS => (ErrorKind::RateLimited, true),
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
            StatusCode::NOT_MODIFIED | StatusCode::PRECONDITION_FAILED => {
                (ErrorKind::ConditionNotMatch, false)
            }
            _ => (ErrorKind::Unexpected, false),
        };

        let message = String::from_utf8_lossy(&bs);

        let mut err = Error::new(kind, message);

        err = with_error_response_context(err, parts);

        if retryable {
            err = err.set_temporary();
        }

        err
    }
}

pub(super) use error::*;
