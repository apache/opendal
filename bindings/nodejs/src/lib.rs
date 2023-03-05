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

use time::format_description::well_known::Rfc3339;
use futures::{TryStreamExt};
use napi::bindgen_prelude::*;
pub use services::{Fs, Memory};
use opendal::Builder;

mod services;

#[napi]
pub struct Operator(opendal::Operator);

#[napi]
impl Operator {
    #[napi(constructor)]
    pub fn new(service_type: String, options: Option<HashMap<String, String>>) -> Result<Self> {
        let ops = options.unwrap_or_default();
        match service_type.as_str() {
            "fs" => Ok(Self(opendal::Operator::create(opendal::services::Fs::from_map(ops))
                .unwrap()
                .finish()
            )),
            "memory" => Ok(Self(opendal::Operator::create(opendal::services::Memory::default())
                       .unwrap()
                       .finish()
            )),
            _ => Err(Error::from_reason("wrong operator type"))
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
pub struct ObjectMetadata(opendal::ObjectMetadata);

#[napi]
impl ObjectMetadata {
    /// Mode of this object.
    #[napi(getter)]
    pub fn mode(&self) -> ObjectMode {
        match self.0.mode() {
            opendal::ObjectMode::DIR => ObjectMode::DIR,
            opendal::ObjectMode::FILE => ObjectMode::FILE,
            opendal::ObjectMode::Unknown => ObjectMode::Unknown,
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
        self.0.last_modified()
            .map(|ta| {
                ta.format(&Rfc3339).unwrap()
            })
    }
}

#[napi]
pub struct ObjectLister(opendal::ObjectLister);

#[napi]
impl ObjectLister {
    #[napi]
    pub async unsafe fn next(&mut self) -> Result<Option<DataObject>> {
        Ok(self.0
            .try_next()
            .await
            .map_err(format_napi_error)
            .unwrap()
            .map(|o| DataObject(o)))
    }
}

#[napi]
pub struct DataObject(opendal::Object);

#[napi]
impl DataObject {
    #[napi]
    pub async fn stat(&self) -> Result<ObjectMetadata> {
        let meta = self.0
            .stat()
            .await
            .map_err(format_napi_error)
            .unwrap();

        Ok(ObjectMetadata(meta))
    }

    #[napi(js_name="statSync")]
    pub fn blocking_stat(&self) -> Result<ObjectMetadata> {
        let meta = self.0
            .blocking_stat()
            .map_err(format_napi_error)
            .unwrap();

        Ok(ObjectMetadata(meta))
    }

    #[napi]
    pub async fn write(&self, content: Buffer) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.0
            .write(c)
            .await
            .map_err(format_napi_error)
    }

    #[napi(js_name="writeSync")]
    pub fn blocking_write(&self, content: Buffer) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.0.blocking_write(c).map_err(format_napi_error)
    }

    #[napi]
    pub async fn read(&self) -> Result<Buffer> {
        let res = self.0
            .read()
            .await
            .map_err(format_napi_error)?;
        Ok(res.into())
    }

    #[napi(js_name="readSync")]
    pub fn blocking_read(&self) -> Result<Buffer> {
        let res = self.0
            .blocking_read()
            .map_err(format_napi_error)?;
        Ok(res.into())
    }

    #[napi]
    pub async fn scan(&self) -> Result<ObjectLister> {
        Ok(ObjectLister(self.0
                .scan()
                .await
                .map_err(format_napi_error)
                .unwrap()
        ))
    }

    #[napi]
    pub async fn delete(&self) -> Result<()> {
        self.0
            .delete()
            .await
            .map_err(format_napi_error)
    }

    #[napi(js_name="deleteSync")]
    pub fn blocking_delete(&self) -> Result<()> {
        self.0
            .blocking_delete()
            .map_err(format_napi_error)
    }
}

fn format_napi_error(err: opendal::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
