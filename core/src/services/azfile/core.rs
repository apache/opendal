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

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;

use http::header::CONTENT_DISPOSITION;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::header::RANGE;
use http::HeaderName;
use http::HeaderValue;
use http::Request;
use http::StatusCode;
use madsim::net::rpc::Deserialize;
use reqsign::AzureStorageCredential;
use reqsign::AzureStorageLoader;
use reqsign::AzureStorageSigner;

use crate::raw::*;
use crate::services::azfile::error::parse_error;
use crate::*;

const X_MS_VERSION: &str = "x-ms-version";
const X_MS_WRITE: &str = "x-ms-write";
const X_MS_FILE_RENAME_SOURCE: &str = "x-ms-file-rename-source";
const X_MS_CONTENT_LENGTH: &str = "x-ms-content-length";
const X_MS_TYPE: &str = "x-ms-type";
const X_MS_FILE_RENAME_REPLACE_IF_EXISTS: &str = "x-ms-file-rename-replace-if-exists";

pub struct AzfileCore {
    pub root: String,
    pub endpoint: String,
    pub share_name: String,
    pub client: HttpClient,
    pub loader: AzureStorageLoader,
    pub signer: AzureStorageSigner,
}

impl Debug for AzfileCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzfileCore")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("share_name", &self.share_name)
            .finish_non_exhaustive()
    }
}

impl AzfileCore {
    async fn load_credential(&self) -> Result<AzureStorageCredential> {
        let cred = self
            .loader
            .load()
            .await
            .map_err(new_request_credential_error)?;

        if let Some(cred) = cred {
            Ok(cred)
        } else {
            Err(Error::new(
                ErrorKind::ConfigInvalid,
                "no valid credential found",
            ))
        }
    }

    pub async fn sign<T>(&self, req: &mut Request<T>) -> Result<()> {
        let cred = self.load_credential().await?;
        // Insert x-ms-version header for normal requests.
        req.headers_mut().insert(
            HeaderName::from_static(X_MS_VERSION),
            // consistent with azdls and azblob
            HeaderValue::from_static("2022-11-02"),
        );
        self.signer.sign(req, &cred).map_err(new_request_sign_error)
    }

    pub async fn azfile_read(
        &self,
        path: &str,
        range: BytesRange,
        buf: &mut oio::WritableBuf,
    ) -> Result<usize> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(RANGE, range.to_header());
        }

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

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

    pub async fn azfile_create_file(&self, path: &str, size: usize, args: &OpWrite) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();
        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        // x-ms-content-length specifies the maximum size for the file, up to 4 tebibytes (TiB)
        // https://learn.microsoft.com/en-us/rest/api/storageservices/create-file
        req = req.header(X_MS_CONTENT_LENGTH, size);

        req = req.header(X_MS_TYPE, "file");

        // Content length must be 0 for create request.
        req = req.header(CONTENT_LENGTH, 0);

        if let Some(ty) = args.content_type() {
            req = req.header(CONTENT_TYPE, ty);
        }

        if let Some(pos) = args.content_disposition() {
            req = req.header(CONTENT_DISPOSITION, pos);
        }

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK | StatusCode::CREATED => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?.with_operation("Backend::azfile_create_file"))
            }
        }
    }

    pub async fn azfile_update(
        &self,
        path: &str,
        size: u64,
        position: u64,
        body: RequestBody,
    ) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?comp=range",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, size);

        req = req.header(X_MS_WRITE, "update");

        req = req.header(
            RANGE,
            BytesRange::from(position..position + size).to_header(),
        );

        let mut req = req.body(body).map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK | StatusCode::CREATED => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?.with_operation("Backend::azfile_update"))
            }
        }
    }

    pub async fn azfile_get_file_properties(&self, path: &str) -> Result<Metadata> {
        let p = build_abs_path(&self.root, path);
        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let meta = parse_into_metadata(path, &parts.headers)?;
                Ok(meta)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn azfile_get_directory_properties(&self, path: &str) -> Result<Metadata> {
        let p = build_abs_path(&self.root, path);

        let url = format!(
            "{}/{}/{}?restype=directory",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let req = Request::head(&url);

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();

        match parts.status {
            StatusCode::OK => {
                let meta = parse_into_metadata(path, &parts.headers)?;
                Ok(meta)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn azfile_rename(&self, path: &str, new_path: &str) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let new_p = build_abs_path(&self.root, new_path)
            .trim_start_matches('/')
            .to_string();

        let url = if path.ends_with('/') {
            format!(
                "{}/{}/{}?restype=directory&comp=rename",
                self.endpoint,
                self.share_name,
                percent_encode_path(&new_p)
            )
        } else {
            format!(
                "{}/{}/{}?comp=rename",
                self.endpoint,
                self.share_name,
                percent_encode_path(&new_p)
            )
        };

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, 0);

        // x-ms-file-rename-source specifies the file or directory to be renamed.
        // the value must be a URL style path
        // the official document does not mention the URL style path
        // find the solution from the community FAQ and implementation of the Java-SDK
        // ref: https://learn.microsoft.com/en-us/answers/questions/799611/azure-file-service-rest-api(rename)?page=1
        let source_url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        req = req.header(X_MS_FILE_RENAME_SOURCE, &source_url);

        req = req.header(X_MS_FILE_RENAME_REPLACE_IF_EXISTS, "true");

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn azfile_create_dir(&self, path: &str) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?restype=directory",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let mut req = Request::put(&url);

        req = req.header(CONTENT_LENGTH, 0);

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                // we cannot just check status code because 409 Conflict has two meaning:
                // 1. If a directory by the same name is being deleted when Create Directory is called, the server returns status code 409 (Conflict)
                // 2. If a directory or file with the same name already exists, the operation fails with status code 409 (Conflict).
                // but we just need case 2 (already exists)
                // ref: https://learn.microsoft.com/en-us/rest/api/storageservices/create-directory
                if parse_header_to_str(&parts.headers, "x-ms-error-code")?.unwrap_or_default()
                    == "ResourceAlreadyExists"
                {
                    body.consume().await?;
                    Ok(())
                } else {
                    let bs = body.to_bytes().await?;
                    Err(parse_error(parts, bs)?)
                }
            }
        }
    }

    pub async fn azfile_delete_file(&self, path: &str) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn azfile_delete_dir(&self, path: &str) -> Result<()> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let url = format!(
            "{}/{}/{}?restype=directory",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p)
        );

        let req = Request::delete(&url);

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn azfile_list(
        &self,
        path: &str,
        limit: &Option<usize>,
        continuation: &String,
    ) -> Result<Option<EnumerationResults>> {
        let p = build_abs_path(&self.root, path)
            .trim_start_matches('/')
            .to_string();

        let mut url = format!(
            "{}/{}/{}?restype=directory&comp=list&include=Timestamps,ETag",
            self.endpoint,
            self.share_name,
            percent_encode_path(&p),
        );

        if !continuation.is_empty() {
            write!(url, "&marker={}", &continuation).expect("write into string must succeed");
        }

        if let Some(limit) = limit {
            write!(url, "&maxresults={}", limit).expect("write into string must succeed");
        }

        let req = Request::get(&url);

        let mut req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;
        self.sign(&mut req).await?;
        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let results = body.to_xml().await?;
                Ok(Some(results))
            }
            StatusCode::NOT_FOUND => {
                body.consume().await?;
                Ok(None)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn ensure_parent_dir_exists(&self, path: &str) -> Result<()> {
        let mut dirs = VecDeque::default();
        // azure file service does not support recursive directory creation
        let mut p = path;
        while p != "/" {
            p = get_parent(p);
            dirs.push_front(p);
        }

        let mut pop_dir_count = dirs.len();
        for dir in dirs.iter().rev() {
            let resp = self.azfile_get_directory_properties(dir).await;
            match resp {
                Ok(_) => break,
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    pop_dir_count -= 1;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        for dir in dirs.iter().skip(pop_dir_count) {
            self.azfile_create_dir(dir).await?;
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct EnumerationResults {
    pub marker: Option<String>,
    pub prefix: Option<String>,
    pub max_results: Option<u32>,
    pub directory_id: Option<String>,
    pub entries: Entries,
    #[serde(default)]
    pub next_marker: String,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct Entries {
    #[serde(default)]
    pub file: Vec<File>,
    #[serde(default)]
    pub directory: Vec<Directory>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct File {
    #[serde(rename = "FileId")]
    pub file_id: String,
    pub name: String,
    pub properties: Properties,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct Directory {
    #[serde(rename = "FileId")]
    pub file_id: String,
    pub name: String,
    pub properties: Properties,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct Properties {
    #[serde(rename = "Content-Length")]
    pub content_length: Option<u64>,
    #[serde(rename = "CreationTime")]
    pub creation_time: String,
    #[serde(rename = "LastAccessTime")]
    pub last_access_time: String,
    #[serde(rename = "LastWriteTime")]
    pub last_write_time: String,
    #[serde(rename = "ChangeTime")]
    pub change_time: String,
    #[serde(rename = "Last-Modified")]
    pub last_modified: String,
    #[serde(rename = "Etag")]
    pub etag: String,
}

#[cfg(test)]
mod tests {
    use quick_xml::de::from_str;

    use super::*;

    #[test]
    fn test_parse_list_result() {
        let xml = r#"
<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://myaccount.file.core.windows.net/" ShareName="myshare" ShareSnapshot="date-time" DirectoryPath="directory-path">
  <Marker>string-value</Marker>
  <Prefix>string-value</Prefix>
  <MaxResults>100</MaxResults>
  <DirectoryId>directory-id</DirectoryId>
  <Entries>
     <File>
        <Name>Rust By Example.pdf</Name>
        <FileId>13835093239654252544</FileId>
        <Properties>
            <Content-Length>5832374</Content-Length>
            <CreationTime>2023-09-25T12:43:05.8483527Z</CreationTime>
            <LastAccessTime>2023-09-25T12:43:05.8483527Z</LastAccessTime>
            <LastWriteTime>2023-09-25T12:43:08.6337775Z</LastWriteTime>
            <ChangeTime>2023-09-25T12:43:08.6337775Z</ChangeTime>
            <Last-Modified>Mon, 25 Sep 2023 12:43:08 GMT</Last-Modified>
            <Etag>\"0x8DBBDC4F8AC4AEF\"</Etag>
        </Properties>
    </File>
    <Directory>
        <Name>test_list_rich_dir</Name>
        <FileId>12105702186650959872</FileId>
        <Properties>
            <CreationTime>2023-10-15T12:03:40.7194774Z</CreationTime>
            <LastAccessTime>2023-10-15T12:03:40.7194774Z</LastAccessTime>
            <LastWriteTime>2023-10-15T12:03:40.7194774Z</LastWriteTime>
            <ChangeTime>2023-10-15T12:03:40.7194774Z</ChangeTime>
            <Last-Modified>Sun, 15 Oct 2023 12:03:40 GMT</Last-Modified>
            <Etag>\"0x8DBCD76C58C3E96\"</Etag>
        </Properties>
    </Directory>
  </Entries>
  <NextMarker />
</EnumerationResults>
        "#;

        let results: EnumerationResults = from_str(xml).unwrap();

        assert_eq!(results.entries.file[0].name, "Rust By Example.pdf");

        assert_eq!(
            results.entries.file[0].properties.etag,
            "\\\"0x8DBBDC4F8AC4AEF\\\""
        );

        assert_eq!(results.entries.directory[0].name, "test_list_rich_dir");

        assert_eq!(
            results.entries.directory[0].properties.etag,
            "\\\"0x8DBCD76C58C3E96\\\""
        );
    }
}
