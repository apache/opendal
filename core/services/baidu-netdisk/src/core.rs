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

use crate::auth::{self, BaiduNetdiskSigner};
use bytes::Buf;
use http::{Request, Response, StatusCode, header};
use opendal_core::{raw::*, *};
use serde::Deserialize;
use serde_json::{Value, json};

pub const FILE_API: &str = "https://pan.baidu.com/rest/2.0/xpan/file";

#[derive(Debug)]
pub struct BaiduNetdiskCore {
    pub info: ServiceInfo,
    pub root: String,
    pub signer: BaiduNetdiskSigner,
}

impl BaiduNetdiskCore {
    pub fn absolute_path(&self, path: &str) -> String {
        build_rooted_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string()
    }
    pub async fn sign<T>(&self, ctx: &OperationContext, req: Request<T>) -> Result<Request<T>> {
        auth::sign(req, &self.signer.token(ctx).await?)
    }
    pub async fn send(&self, ctx: &OperationContext, req: Request<Buffer>) -> Result<Value> {
        let (parts, body) = req.into_parts();
        let token = self.signer.token(ctx).await?;
        let request = auth::sign(Request::from_parts(parts.clone(), body.clone()), &token)?;
        let response = ctx.http_transport().send(request).await?;
        let status = response.status();
        let value: Value = serde_json::from_reader(response.into_body().reader())
            .map_err(new_json_deserialize_error)?;
        // Retry once only when the API explicitly rejected an expired access token.
        // filemanager errno=111 instead means that another async task is running.
        if value["error_code"].as_i64() == Some(111) && self.signer.invalidate(&token).await {
            let request = self.sign(ctx, Request::from_parts(parts, body)).await?;
            return parse_response(ctx.http_transport().send(request).await?);
        }
        check_response(status, value)
    }
    pub async fn stat(&self, ctx: &OperationContext, path: &str) -> Result<FileMetadata> {
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("method", "meta")
            .append_pair("path", &self.absolute_path(path))
            .finish();
        let req = Request::get(format!("{FILE_API}?{query}"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let response = self.send(ctx, req).await?;
        let result: FileList =
            serde_json::from_value(response).map_err(new_json_deserialize_error)?;
        let file =
            result.list.into_iter().next().ok_or_else(|| {
                Error::new(ErrorKind::NotFound, "Baidu Netdisk path does not exist")
            })?;
        if path.ends_with('/') && file.isdir != 1 {
            return Err(Error::new(ErrorKind::NotFound, "path is not a directory"));
        }
        Ok(file)
    }
    pub async fn create_dir(&self, ctx: &OperationContext, path: &str) -> Result<()> {
        let form = form_urlencoded::Serializer::new(String::new())
            .append_pair("path", &self.absolute_path(path))
            .append_pair("isdir", "1")
            .append_pair("rtype", "0")
            .finish();
        let req = Request::post(format!("{FILE_API}?method=create"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(form))
            .map_err(new_request_build_error)?;
        match self.send(ctx, req).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                if self.stat(ctx, path).await?.isdir == 1 {
                    Ok(())
                } else {
                    Err(Error::new(ErrorKind::NotADirectory, "path is a file"))
                }
            }
            Err(e) => Err(e),
        }
    }
    pub async fn manage(
        &self,
        ctx: &OperationContext,
        operation: &str,
        files: Value,
    ) -> Result<()> {
        let form = form_urlencoded::Serializer::new(String::new())
            .append_pair("async", "0")
            .append_pair("ondup", "overwrite")
            .append_pair("filelist", &files.to_string())
            .finish();
        let req = Request::post(format!("{FILE_API}?method=filemanager&opera={operation}"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(form))
            .map_err(new_request_build_error)?;
        let result = self.send(ctx, req).await?;
        if result.get("taskid").is_some() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Baidu Netdisk returned an asynchronous task for a synchronous request",
            ));
        }
        for item in result["info"]
            .as_array()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "missing filemanager results"))?
        {
            if let Some(code) = item["errno"].as_i64().filter(|code| *code != 0) {
                return Err(api_error(code));
            }
        }
        Ok(())
    }
    pub async fn transfer(
        &self,
        ctx: &OperationContext,
        operation: &str,
        from: &str,
        to: &str,
    ) -> Result<()> {
        self.create_dir(ctx, get_parent(to)).await?;
        self.manage(ctx, operation, json!([{"path":self.absolute_path(from),
            "dest":self.absolute_path(get_parent(to)), "newname":get_basename(to), "ondup":"overwrite"}])).await
    }
}

#[derive(Deserialize)]
pub struct FileList {
    pub list: Vec<FileMetadata>,
}

#[derive(Deserialize)]
pub struct FileMetadata {
    pub fs_id: u64,
    pub path: String,
    pub isdir: u8,
    #[serde(default)]
    pub size: u64,
    pub mtime: Option<i64>,
    pub server_mtime: Option<i64>,
    pub dlink: Option<String>,
}

impl FileMetadata {
    pub fn metadata(&self) -> Result<Metadata> {
        let mut metadata = if self.isdir == 1 {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::file(self.size)
        };
        if let Some(time) = self.server_mtime.or(self.mtime) {
            metadata.last_modified(Timestamp::from_second(time).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "invalid Baidu Netdisk modification time",
                )
                .set_source(e)
            })?);
        }
        Ok(metadata.build())
    }
}

pub fn api_error(code: i64) -> Error {
    let kind = match code {
        -9 | 31066 | 31190 => ErrorKind::NotFound,
        -8 => ErrorKind::AlreadyExists,
        -7 | 6 | 110 | 31024 | 31045 | 20011 => ErrorKind::PermissionDenied,
        31034 => ErrorKind::RateLimited,
        _ => ErrorKind::Unexpected,
    };
    let error = Error::new(kind, "Baidu Netdisk API request failed")
        .with_context("errno", code.to_string());
    if code == 111 || code == 31034 {
        error.set_temporary()
    } else {
        error
    }
}

pub fn parse_response(response: Response<Buffer>) -> Result<Value> {
    let status = response.status();
    let value: Value = serde_json::from_reader(response.into_body().reader())
        .map_err(new_json_deserialize_error)?;
    check_response(status, value)
}

fn check_response(status: StatusCode, value: Value) -> Result<Value> {
    if value["error_code"].as_i64() == Some(111) {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "Baidu Netdisk access token expired",
        ));
    }
    if value["errno"].as_i64() == Some(12)
        && let Some(items) = value["info"].as_array()
        && let Some(code) = items
            .iter()
            .filter_map(|item| item["errno"].as_i64())
            .find(|code| *code != 0)
    {
        return Err(api_error(code));
    }
    if let Some(code) = value["errno"]
        .as_i64()
        .or_else(|| value["error_code"].as_i64())
        .filter(|code| *code != 0)
    {
        if code == -7
            && let Some(chars) = value["illegal_chars"].as_str().filter(|v| !v.is_empty())
        {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "Baidu Netdisk rejected characters in the file name",
            )
            .with_context("errno", "-7")
            .with_context("illegal_chars", chars.to_string()));
        }
        return Err(api_error(code));
    }
    if !status.is_success() {
        let kind = match status {
            StatusCode::NOT_FOUND => ErrorKind::NotFound,
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => ErrorKind::PermissionDenied,
            StatusCode::TOO_MANY_REQUESTS => ErrorKind::RateLimited,
            _ => ErrorKind::Unexpected,
        };
        let error = Error::new(kind, "Baidu Netdisk HTTP request failed")
            .with_context("status", status.as_str());
        return Err(
            if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
                error.set_temporary()
            } else {
                error
            },
        );
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_expired_token_from_busy_filemanager() {
        let expired =
            check_response(StatusCode::UNAUTHORIZED, json!({"error_code": 111})).unwrap_err();
        assert_eq!(expired.kind(), ErrorKind::PermissionDenied);
        assert!(!expired.is_temporary());
        let busy = check_response(StatusCode::OK, json!({"errno": 111})).unwrap_err();
        assert_eq!(busy.kind(), ErrorKind::Unexpected);
        assert!(busy.is_temporary());
    }
}
