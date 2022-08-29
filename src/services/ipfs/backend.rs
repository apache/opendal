// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::builder::Builder;
use super::dir_stream::DirStream;
use super::error::parse_error;
use super::rpc::IpfsRpcV0;

use crate::io_util::unshared_reader;
use crate::ops::{OpCreate, OpDelete, OpList, OpRead, OpStat, OpWrite, Operation};
use crate::{
    Accessor, AccessorMetadata, BytesReader, DirStreamer, ObjectMetadata, ObjectMode, Scheme,
};

use async_trait::async_trait;
use futures::AsyncReadExt;
use http::StatusCode;
use serde::Deserialize;
use std::fmt;
use std::io;
use std::str;
use std::sync::Arc;

use crate::http_util::{
    new_request_build_error, new_request_send_error, new_response_consume_error,
    parse_error_response, percent_encode_path, HttpClient,
};

use isahc;
use isahc::AsyncReadResponseExt;

/// Backend for IPFS service
#[derive(Clone)]
pub struct Backend {
    root: String,
    endpoint: String,
    client: HttpClient,
}

#[derive(Deserialize, Debug)]
struct StatBody {
    #[serde(rename(deserialize = "Size"))]
    size: u64,
    #[serde(rename(deserialize = "Type"))]
    file_type: String,
}

impl fmt::Debug for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ipfs::Backend")
            .field("root", &self.root)
            .finish()
    }
}

impl Backend {
    /// Create a default builder for ipfs.
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn new(root: String, client: HttpClient, endpoint: String) -> Self {
        Self {
            root,
            client,
            endpoint,
        }
    }

    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> io::Result<Self> {
        let mut builder = Builder::default();

        for (key, val) in it {
            let val = val.as_str();
            match key.as_ref() {
                "root" => builder.root(val),
                "endpoint" => builder.endpoint(val),
                _ => continue,
            };
        }

        builder.build()
    }

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == self.root {
            return path.to_string();
        }

        format!("{}{}", self.root, path.trim_start_matches(&self.root))
    }

    pub(crate) async fn files_stat(&self, path: &str) -> io::Result<ObjectMetadata> {
        let query_string = self.new_query_string(path);

        let mut resp = self
            .make_request(&query_string, IpfsRpcV0::Stat, path, Operation::Stat)
            .await?;

        let resp_bytes = resp.bytes().await?;

        let res: StatBody = serde_json::from_slice(&resp_bytes)?;

        let mode: ObjectMode = match res.file_type.as_str() {
            "file" => ObjectMode::FILE,
            "directory" => ObjectMode::DIR,
            _ => ObjectMode::Unknown,
        };

        let mut meta = ObjectMetadata::default();
        meta.set_mode(mode).set_content_length(res.size);

        Ok(meta)
    }

    pub(crate) async fn files_create(&self, path: &str) -> io::Result<()> {
        let query_string = format!("{}&create=true&truncate=false", self.new_query_string(path));

        self.make_write_request(
            &query_string,
            "",
            path,
            IpfsRpcV0::Create,
            Operation::Create,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn files_read(
        &self,
        path: &str,
        offset: Option<i64>,
        count: Option<i64>,
    ) -> io::Result<BytesReader> {
        let mut query_string = self.new_query_string(path);
        if offset.is_some() {
            query_string = format!("{}&offset={}", query_string, offset.unwrap());
        }
        if count.is_some() {
            query_string = format!("{}&count={}", query_string, count.unwrap());
        }

        let resp = self
            .make_request(&query_string, IpfsRpcV0::Read, path, Operation::Read)
            .await?;

        Ok(Box::new(resp.into_body()))
    }

    pub(crate) async fn files_delete(&self, path: &str) -> io::Result<()> {
        let query_string = self.new_query_string(path);

        self.make_request(&query_string, IpfsRpcV0::Delete, path, Operation::Delete)
            .await?;
        Ok(())
    }

    pub(crate) async fn files_list(
        &self,
        path: &str,
    ) -> io::Result<isahc::Response<isahc::AsyncBody>> {
        let query_string = self.new_query_string(path);

        let resp = self
            .make_request(&query_string, IpfsRpcV0::List, path, Operation::List)
            .await?;
        Ok(resp)
    }

    pub(crate) async fn files_write(
        &self,
        path: &str,
        data: &str,
    ) -> io::Result<isahc::Response<isahc::AsyncBody>> {
        let query_string = format!("{}&create=true&truncate=false", self.new_query_string(path));

        let resp = self
            .make_write_request(
                &query_string,
                data,
                path,
                IpfsRpcV0::Write,
                Operation::Write,
            )
            .await?;

        Ok(resp)
    }

    async fn make_request(
        &self,
        query_string: &str,
        action: IpfsRpcV0,
        path: &str,
        op: Operation,
    ) -> io::Result<isahc::Response<isahc::AsyncBody>> {
        let url = format!("{}{}?{}", self.endpoint, action.to_path(), query_string);

        let req = isahc::Request::post(url);
        let req = req
            .body(isahc::AsyncBody::empty())
            .map_err(|err| new_request_build_error(op, path, err))?;

        let resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(op, path, e))?;

        Ok(resp)
    }

    async fn make_write_request(
        &self,
        query_string: &str,
        data: &str,
        path: &str,
        action: IpfsRpcV0,
        op: Operation,
    ) -> io::Result<isahc::Response<isahc::AsyncBody>> {
        let url = format!("{}{}?{}", self.endpoint, action.to_path(), query_string);
        let mut req = isahc::Request::post(url);

        req = req.header(
            http::header::CONTENT_TYPE,
            "multipart/form-data; boundary=custom-boundary",
        );
        let left = "--custom-boundary\nContent-Disposition: form-data; name=data;\n\n";
        let right = "\n--custom-boundary";

        let buff = format!("{}{}{}", left, data, right);

        let body = isahc::AsyncBody::from_bytes_static(buff);
        let req = req
            .body(body)
            .map_err(|err| new_request_build_error(op, path, err))?;

        let resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(op, path, e))?;

        Ok(resp)
    }

    fn new_query_string(&self, path: &str) -> String {
        let encoded_path = percent_encode_path(path);
        format!("arg={}", encoded_path)
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Ipfs)
            .set_root(&self.root)
            .set_capabilities(None);

        am
    }

    async fn create(&self, args: &OpCreate) -> io::Result<()> {
        let path = self.get_abs_path(args.path());
        self.files_create(&path).await
    }

    async fn read(&self, args: &OpRead) -> io::Result<BytesReader> {
        let path = self.get_abs_path(args.path());

        let offset = args.offset().and_then(|val| i64::try_from(val).ok());
        let size = args.size().and_then(|val| i64::try_from(val).ok());
        let reader = self.files_read(&path, offset, size).await?;
        Ok(reader)
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> io::Result<u64> {
        let path = self.get_abs_path(args.path());

        let mut string_buff: String = "".to_string();
        unshared_reader(r).read_to_string(&mut string_buff).await?;

        let mut resp = self.files_write(&path, &string_buff).await?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                resp.consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Write, &path, err))?;
                Ok(args.size())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Write, args.path(), er);
                Err(err)
            }
        }
    }

    async fn stat(&self, args: &OpStat) -> io::Result<ObjectMetadata> {
        let path = self.get_abs_path(args.path());
        self.files_stat(&path).await
    }

    async fn delete(&self, args: &OpDelete) -> io::Result<()> {
        let path = self.get_abs_path(args.path());
        self.files_delete(&path).await
    }

    async fn list(&self, args: &OpList) -> io::Result<DirStreamer> {
        let path = self.get_abs_path(args.path());
        Ok(Box::new(DirStream::new(Arc::new(self.clone()), &path)))
    }
}
