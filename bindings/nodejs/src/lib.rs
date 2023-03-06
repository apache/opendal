// Copyright 2022 Datafuse Labs
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

#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

use std::collections::HashMap;
use std::str;

use futures::TryStreamExt;
use napi::bindgen_prelude::*;
use opendal::Builder;
use time::format_description::well_known::Rfc3339;

#[napi]
pub struct Operator(opendal::Operator);

#[napi]
#[derive(Debug, Eq, PartialEq)]
pub enum Scheme {
    /// [azblob][crate::services::Azblob]: Azure Storage Blob services.
    Azblob,
    /// [azdfs][crate::services::Azdfs]: Azure Data Lake Storage Gen2.
    Azdfs,
    /// [dashmap][crate::services::Dashmap]: dashmap backend support.
    Dashmap,
    /// [fs][crate::services::Fs]: POSIX alike file system.
    Fs,
    /// [gcs][crate::services::Gcs]: Google Cloud Storage backend.
    Gcs,
    /// [ghac][crate::services::Ghac]: Github Action Cache services.
    Ghac,
    /// [hdfs][crate::services::Hdfs]: Hadoop Distributed File System.
    Hdfs,
    /// [http][crate::services::Http]: HTTP backend.
    Http,
    /// [ftp][crate::services::Ftp]: FTP backend.
    Ftp,
    /// [ipmfs][crate::services::Ipfs]: IPFS HTTP Gateway
    Ipfs,
    /// [ipmfs][crate::services::Ipmfs]: IPFS mutable file system
    Ipmfs,
    /// [memcached][crate::services::Memcached]: Memcached service support.
    Memcached,
    /// [memory][crate::services::Memory]: In memory backend support.
    Memory,
    /// [moka][crate::services::Moka]: moka backend support.
    Moka,
    /// [obs][crate::services::Obs]: Huawei Cloud OBS services.
    Obs,
    /// [oss][crate::services::Oss]: Aliyun Object Storage Services
    Oss,
    /// [redis][crate::services::Redis]: Redis services
    Redis,
    /// [rocksdb][crate::services::Rocksdb]: RocksDB services
    Rocksdb,
    /// [s3][crate::services::S3]: AWS S3 alike services.
    S3,
    /// [sled][crate::services::Sled]: Sled services
    Sled,
    /// [webdav][crate::services::Webdav]: WebDAV support.
    Webdav,
    /// [webhdfs][crate::services::Webhdfs]: WebHDFS RESTful API Services
    Webhdfs,
}

#[napi]
impl Operator {
    #[napi(constructor)]
    pub fn new(service_type: Scheme, options: Option<HashMap<String, String>>) -> Result<Self> {
        let ops = options.unwrap_or_default();
        match service_type {
            Scheme::Fs => Ok(Self(
                opendal::Operator::create(opendal::services::Fs::from_map(ops))
                    .unwrap()
                    .finish(),
            )),
            Scheme::Memory => Ok(Self(
                opendal::Operator::create(opendal::services::Memory::default())
                    .unwrap()
                    .finish(),
            )),
            _ => Err(Error::from_reason("wrong operator type")),
        }
    }

    #[napi]
    pub fn object(&self, path: String) -> DataObject {
        DataObject(self.0.object(&path))
    }
}

#[napi]
pub enum ObjectMode {
    /// FILE means the object has data to read.
    FILE,
    /// DIR means the object can be listed.
    DIR,
    /// Unknown means we don't know what we can do on this object.
    Unknown,
}

#[allow(dead_code)]
#[napi]
pub struct ObjectMetadata(opendal::Metadata);

#[napi]
impl ObjectMetadata {
    /// Mode of this object.
    #[napi(getter)]
    pub fn mode(&self) -> ObjectMode {
        match self.0.mode() {
            opendal::EntryMode::DIR => ObjectMode::DIR,
            opendal::EntryMode::FILE => ObjectMode::FILE,
            opendal::EntryMode::Unknown => ObjectMode::Unknown,
        }
    }

    /// Content-Disposition of this object
    #[napi(getter)]
    pub fn content_disposition(&self) -> Option<String> {
        self.0.content_disposition().map(|s| s.to_string())
    }

    /// Content Length of this object
    #[napi(getter)]
    pub fn content_length(&self) -> Option<u64> {
        self.0.content_length().into()
    }

    /// Content MD5 of this object.
    #[napi(getter)]
    pub fn content_md5(&self) -> Option<String> {
        self.0.content_md5().map(|s| s.to_string())
    }

    // /// Content Range of this object.
    // /// API undecided.
    // #[napi(getter)]
    // pub fn content_range(&self) -> Option<Vec<u32>> {
    //     todo!()
    // }

    /// Content Type of this object.
    #[napi(getter)]
    pub fn content_type(&self) -> Option<String> {
        self.0.content_type().map(|s| s.to_string())
    }

    /// ETag of this object.
    #[napi(getter)]
    pub fn etag(&self) -> Option<String> {
        self.0.etag().map(|s| s.to_string())
    }

    /// Last Modified of this object.(UTC)
    #[napi(getter)]
    pub fn last_modified(&self) -> Option<String> {
        self.0
            .last_modified()
            .map(|ta| ta.format(&Rfc3339).unwrap())
    }
}

#[napi]
pub struct ObjectLister(opendal::Lister);

#[napi]
impl ObjectLister {
    /// # Safety
    ///
    /// > &mut self in async napi methods should be marked as unsafe
    ///
    /// napi will make sure the function is safe, and we didn't do unsafe
    /// thing internally.
    #[napi]
    pub async unsafe fn next(&mut self) -> Result<Option<DataObject>> {
        Ok(self
            .0
            .try_next()
            .await
            .map_err(format_napi_error)
            .unwrap()
            .map(DataObject))
    }
}

#[napi]
pub struct DataObject(opendal::Object);

#[napi]
impl DataObject {
    #[napi]
    pub async fn stat(&self) -> Result<ObjectMetadata> {
        let meta = self.0.stat().await.map_err(format_napi_error).unwrap();

        Ok(ObjectMetadata(meta))
    }

    #[napi(js_name = "statSync")]
    pub fn blocking_stat(&self) -> Result<ObjectMetadata> {
        let meta = self.0.blocking_stat().map_err(format_napi_error).unwrap();

        Ok(ObjectMetadata(meta))
    }

    #[napi]
    pub async fn write(&self, content: Buffer) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.0.write(c).await.map_err(format_napi_error)
    }

    #[napi(js_name = "writeSync")]
    pub fn blocking_write(&self, content: Buffer) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.0.blocking_write(c).map_err(format_napi_error)
    }

    #[napi]
    pub async fn read(&self) -> Result<Buffer> {
        let res = self.0.read().await.map_err(format_napi_error)?;
        Ok(res.into())
    }

    #[napi(js_name = "readSync")]
    pub fn blocking_read(&self) -> Result<Buffer> {
        let res = self.0.blocking_read().map_err(format_napi_error)?;
        Ok(res.into())
    }

    #[napi]
    pub async fn scan(&self) -> Result<ObjectLister> {
        Ok(ObjectLister(
            self.0.scan().await.map_err(format_napi_error).unwrap(),
        ))
    }

    #[napi]
    pub async fn delete(&self) -> Result<()> {
        self.0.delete().await.map_err(format_napi_error)
    }

    #[napi(js_name = "deleteSync")]
    pub fn blocking_delete(&self) -> Result<()> {
        self.0.blocking_delete().map_err(format_napi_error)
    }
}

fn format_napi_error(err: opendal::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
