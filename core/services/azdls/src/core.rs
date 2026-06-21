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

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use http::HeaderName;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::StatusCode;
use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::IF_MATCH;
use http::header::IF_MODIFIED_SINCE;
use http::header::IF_NONE_MATCH;
use http::header::IF_UNMODIFIED_SINCE;
use reqsign_azure_storage::Credential;
use reqsign_core::{Context, Signer};

use opendal_core::raw::*;
use opendal_core::*;

const X_MS_RENAME_SOURCE: &str = "x-ms-rename-source";
const X_MS_VERSION: &str = "x-ms-version";
pub const X_MS_VERSION_ID: &str = "x-ms-version-id";
const X_MS_CONTINUATION: &str = "x-ms-continuation";
pub const DIRECTORY: &str = "directory";
pub const FILE: &str = "file";
// Format: `n1=base64(v1),n2=base64(v2),...`.
// See https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
const X_MS_PROPERTIES: &str = "x-ms-properties";

pub struct AzdlsCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub filesystem: String,
    pub root: String,
    pub endpoint: String,
    pub enable_hns: bool,

    pub signer: Signer<Credential>,
    pub sign_ctx: Context,
}

impl Debug for AzdlsCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzdlsCore")
            .field("filesystem", &self.filesystem)
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("enable_hns", &self.enable_hns)
            .finish_non_exhaustive()
    }
}

impl AzdlsCore {
    fn signer(&self, ctx: &OperationContext) -> Signer<Credential> {
        self.signer.clone().with_context(
            self.sign_ctx
                .clone()
                .with_http_send(ctx.http_transport().clone()),
        )
    }

    pub async fn sign<T>(&self, ctx: &OperationContext, req: Request<T>) -> Result<Request<T>> {
        let (mut parts, body) = req.into_parts();

        // Insert x-ms-version header for normal requests.
        parts.headers.insert(
            HeaderName::from_static(X_MS_VERSION),
            // 2022-11-02 is the version supported by Azurite V3 and
            // used by Azure Portal, We use this version to make
            // sure most our developer happy.
            //
            // In the future, we could allow users to configure this value.
            HeaderValue::from_static("2022-11-02"),
        );

        self.signer(ctx)
            .sign(&mut parts, None)
            .await
            .map_err(|e| new_request_sign_error(e.into()))?;

        Ok(Request::from_parts(parts, body))
    }

    #[inline]
    pub async fn send(
        &self,
        ctx: &OperationContext,
        req: Request<Buffer>,
    ) -> Result<Response<Buffer>> {
        ctx.http_transport().send(req).await
    }
}

impl AzdlsCore {
    pub async fn azdls_read(
        &self,
        ctx: &OperationContext,
        path: &str,
        range: BytesRange,
        args: &OpRead,
    ) -> Result<Response<HttpBody>> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        if let Some(if_match) = args.if_match() {
            req = req.header(IF_MATCH, if_match);
        }
        if let Some(if_none_match) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, if_none_match);
        }
        if let Some(if_modified_since) = args.if_modified_since() {
            req = req.header(IF_MODIFIED_SINCE, if_modified_since.format_http_date());
        }
        if let Some(if_unmodified_since) = args.if_unmodified_since() {
            req = req.header(IF_UNMODIFIED_SINCE, if_unmodified_since.format_http_date());
        }

        let req = req
            .extension(Operation::Read)
            .extension(ServiceOperation("ReadFile"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        ctx.http_transport().fetch(req).await
    }

    /// resource should be one of `file` or `directory`
    ///
    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create
    pub async fn azdls_create(
        &self,
        ctx: &OperationContext,
        path: &str,
        resource: &str,
        args: &OpWrite,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?resource={resource}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // Content length must be 0 for create request.
        req = req.header(CONTENT_LENGTH, 0);

        if let Some(ty) = args.content_type() {
            req = req.header(CONTENT_TYPE, ty)
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos)
        }

        if args.if_not_exists() {
            req = req.header(IF_NONE_MATCH, "*")
        }

        if let Some(v) = args.if_none_match() {
            req = req.header(IF_NONE_MATCH, v)
        }

        if let Some(user_metadata) = args.user_metadata() {
            if !user_metadata.is_empty() {
                req = req.header(X_MS_PROPERTIES, encode_user_metadata(user_metadata));
            }
        }

        let operation = if resource == DIRECTORY {
            Operation::CreateDir
        } else {
            Operation::Write
        };
        let service_operation = if resource == DIRECTORY {
            ServiceOperation("CreateDirectory")
        } else {
            ServiceOperation("CreateFile")
        };

        let req = req
            .extension(operation)
            .extension(service_operation)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn azdls_rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
    ) -> Result<Response<Buffer>> {
        let source = build_abs_path(&self.root, from);
        let target = build_abs_path(&self.root, to);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&target)
        );

        let source_path = format!("/{}/{}", self.filesystem, percent_encode_path(&source));

        let req = Request::put(&url)
            .header(X_MS_RENAME_SOURCE, source_path)
            .header(CONTENT_LENGTH, 0)
            .extension(Operation::Rename)
            .extension(ServiceOperation("RenamePath"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    /// ref: https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update
    #[allow(clippy::too_many_arguments)]
    pub async fn azdls_append(
        &self,
        ctx: &OperationContext,
        path: &str,
        size: Option<u64>,
        position: u64,
        flush: bool,
        close: bool,
        body: Buffer,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}/{}?action=append&position={}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p),
            position
        );

        if flush {
            url.push_str("&flush=true");
        }
        if close {
            url.push_str("&close=true");
        }

        let mut req = Request::patch(&url);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size)
        }

        let req = req
            .extension(Operation::Write)
            .extension(ServiceOperation("AppendData"))
            .body(body)
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    /// Flush pending data appended by [`azdls_append`].
    pub async fn azdls_flush(
        &self,
        ctx: &OperationContext,
        path: &str,
        position: u64,
        close: bool,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/{}/{}?action=flush&position={}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p),
            position
        );

        if close {
            url.push_str("&close=true");
        }

        let req = Request::patch(&url)
            .header(CONTENT_LENGTH, 0)
            .extension(Operation::Write)
            .extension(ServiceOperation("FlushData"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn azdls_get_properties(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let req = req
            .extension(Operation::Stat)
            .extension(ServiceOperation("GetPathProperties"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn azdls_stat_metadata(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Metadata> {
        let resp = self.azdls_get_properties(ctx, path).await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let headers = resp.headers();
        let mut meta = parse_into_metadata(path, headers)?;

        if let Some(version_id) = parse_header_to_str(headers, X_MS_VERSION_ID)? {
            meta.set_version(version_id);
        }

        if let Some(value) = parse_header_to_str(headers, X_MS_PROPERTIES)? {
            let user_meta = decode_user_metadata(value);
            if !user_meta.is_empty() {
                meta = meta.with_user_metadata(user_meta);
            }
        }

        let resource = resp
            .headers()
            .get("x-ms-resource-type")
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "azdls should return x-ms-resource-type header, but it's missing",
                )
            })?
            .to_str()
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "azdls should return x-ms-resource-type header, but it's not a valid string",
                )
                .set_source(err)
            })?;

        match resource {
            FILE => Ok(meta.with_mode(EntryMode::FILE)),
            DIRECTORY => Ok(meta.with_mode(EntryMode::DIR)),
            v => Err(Error::new(
                ErrorKind::Unexpected,
                "azdls returns an unknown x-ms-resource-type",
            )
            .with_context("resource", v)),
        }
    }

    pub async fn azdls_delete(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url)
            .extension(Operation::Delete)
            .extension(ServiceOperation("DeletePath"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn azdls_recursive_delete(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let base = format!(
            "{}/{}/{}",
            self.endpoint,
            self.filesystem,
            percent_encode_path(&p)
        );

        let mut continuation = String::new();

        loop {
            let mut url = QueryPairsWriter::new(&base).push("recursive", "true");

            if self.enable_hns {
                url = url.push("paginated", "true");
            }

            if !continuation.is_empty() {
                url = url.push("continuation", &percent_encode_path(&continuation));
            }

            let req = Request::delete(url.finish())
                .extension(Operation::Delete)
                .extension(ServiceOperation("RecursiveDeletePath"))
                .body(Buffer::new())
                .map_err(new_request_build_error)?;

            let req = self.sign(ctx, req).await?;
            let resp = self.send(ctx, req).await?;

            let status = resp.status();
            match status {
                StatusCode::OK | StatusCode::ACCEPTED | StatusCode::NOT_FOUND => {}
                _ => return Err(parse_error(resp)),
            }

            let next = resp
                .headers()
                .get(X_MS_CONTINUATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .trim();

            if next.is_empty() {
                return Ok(resp);
            }

            continuation = next.to_string();
        }
    }

    pub async fn azdls_list(
        &self,
        ctx: &OperationContext,
        path: &str,
        continuation: &str,
        limit: Option<usize>,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = QueryPairsWriter::new(&format!("{}/{}", self.endpoint, self.filesystem))
            .push("resource", "filesystem")
            .push("recursive", "false");
        if !p.is_empty() {
            url = url.push("directory", &percent_encode_path(&p));
        }
        if let Some(limit) = limit {
            url = url.push("maxResults", &limit.to_string());
        }
        if !continuation.is_empty() {
            url = url.push("continuation", &percent_encode_path(continuation));
        }

        let req = Request::get(url.finish())
            .extension(Operation::List)
            .extension(ServiceOperation("ListPaths"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let req = self.sign(ctx, req).await?;
        self.send(ctx, req).await
    }

    pub async fn azdls_ensure_parent_path(
        &self,
        ctx: &OperationContext,
        path: &str,
    ) -> Result<Option<Response<Buffer>>> {
        let abs_target_path = path.trim_end_matches('/').to_string();
        let abs_target_path = abs_target_path.as_str();
        let mut parts: Vec<&str> = abs_target_path
            .split('/')
            .filter(|x| !x.is_empty())
            .collect();

        if !parts.is_empty() {
            parts.pop();
        }

        if !parts.is_empty() {
            let parent_path = parts.join("/");
            let resp = self
                .azdls_create(ctx, &parent_path, DIRECTORY, &OpWrite::default())
                .await?;

            Ok(Some(resp))
        } else {
            Ok(None)
        }
    }
}

fn encode_user_metadata(metadata: &HashMap<String, String>) -> String {
    metadata
        .iter()
        .map(|(k, v)| format!("{k}={}", BASE64_STANDARD.encode(v)))
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_user_metadata(value: &str) -> HashMap<String, String> {
    value
        .split(',')
        .filter_map(|pair| {
            let (key, encoded) = pair.split_once('=')?;
            let bytes = BASE64_STANDARD.decode(encoded.trim()).ok()?;
            let decoded = String::from_utf8(bytes).ok()?;
            Some((key.trim().to_string(), decoded))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_then_decode_roundtrips() {
        let mut input = HashMap::new();
        input.insert("location".to_string(), "everywhere".to_string());
        input.insert("owner".to_string(), "opendal".to_string());

        let decoded = decode_user_metadata(&encode_user_metadata(&input));
        assert_eq!(decoded, input);
    }

    #[test]
    fn encoded_value_is_base64_so_special_characters_survive() {
        let mut input = HashMap::new();
        input.insert("k".to_string(), "a=b,c".to_string());

        let decoded = decode_user_metadata(&encode_user_metadata(&input));
        assert_eq!(decoded.get("k"), Some(&"a=b,c".to_string()));
    }

    #[test]
    fn decode_preserves_utf8_multibyte_values() {
        let mut input = HashMap::new();
        input.insert("greeting".to_string(), "你好".to_string());

        let decoded = decode_user_metadata(&encode_user_metadata(&input));
        assert_eq!(decoded, input);
    }

    #[test]
    fn decode_silently_skips_malformed_entries_and_keeps_valid_ones() {
        let header = format!(
            "good=Z29vZA==,no_equals,bad_b64=!!!,k={}",
            BASE64_STANDARD.encode([0xFF, 0xFE])
        );
        let decoded = decode_user_metadata(&header);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.get("good"), Some(&"good".to_string()));
    }
}

mod error {
    use std::fmt::Debug;

    use bytes::Buf;
    use http::Response;
    use http::StatusCode;
    use opendal_core::*;
    use opendal_service_azure_common::with_azure_error_response_context;
    use quick_xml::de;
    use serde::Deserialize;

    /// AzdlsError is the error returned by azure dfs service.
    #[derive(Default, Deserialize)]
    #[serde(default, rename_all = "PascalCase")]
    struct AzdlsError {
        code: String,
        message: String,
        query_parameter_name: String,
        query_parameter_value: String,
        reason: String,
    }

    impl Debug for AzdlsError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let mut de = f.debug_struct("AzdlsError");
            de.field("code", &self.code);
            // replace `\n` to ` ` for better reading.
            de.field("message", &self.message.replace('\n', " "));

            if !self.query_parameter_name.is_empty() {
                de.field("query_parameter_name", &self.query_parameter_name);
            }
            if !self.query_parameter_value.is_empty() {
                de.field("query_parameter_value", &self.query_parameter_value);
            }
            if !self.reason.is_empty() {
                de.field("reason", &self.reason);
            }

            de.finish()
        }
    }

    /// Parse error response into Error.
    pub(crate) fn parse_error(resp: Response<Buffer>) -> Error {
        let (parts, body) = resp.into_parts();
        let bs = body.to_bytes();

        let (kind, retryable) = match parts.status {
            StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
            StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
            StatusCode::PRECONDITION_FAILED | StatusCode::NOT_MODIFIED | StatusCode::CONFLICT => {
                (ErrorKind::ConditionNotMatch, false)
            }
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
            _ => (ErrorKind::Unexpected, false),
        };

        let mut message = match de::from_reader::<_, AzdlsError>(bs.clone().reader()) {
            Ok(azdls_err) => format!("{azdls_err:?}"),
            Err(_) => String::from_utf8_lossy(&bs).into_owned(),
        };
        // If there is no body here, fill with error code.
        if message.is_empty()
            && let Some(v) = parts.headers.get("x-ms-error-code")
            && let Ok(code) = v.to_str()
        {
            message = format!(
                "{:?}",
                AzdlsError {
                    code: code.to_string(),
                    ..Default::default()
                }
            )
        }

        let mut err = Error::new(kind, &message);

        err = with_azure_error_response_context(err, parts);

        if retryable {
            err = err.set_temporary();
        }

        err
    }
}

pub(super) use error::*;
