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

use std::str;

use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use napi::bindgen_prelude::*;

#[napi]
pub struct Memory {}

#[napi]
impl Memory {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {}
    }

    #[napi]
    pub fn build(&self) -> Operator {
        self.make_operator()
    }
}

trait OperatorBuilder {
    fn make_operator(&self) -> Operator;
}

impl OperatorBuilder for Memory {
    fn make_operator(&self) -> Operator {
        Operator(opendal::Operator::create(opendal::services::Memory::default()).unwrap().finish())
    }
}

#[napi]
pub struct Operator(opendal::Operator);

#[napi]
impl Operator {
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
    #[napi]
    pub fn mode(&self) -> ObjectMode {
        match self.0.mode() {
            opendal::ObjectMode::DIR => ObjectMode::DIR,
            opendal::ObjectMode::FILE => ObjectMode::FILE,
            opendal::ObjectMode::Unknown => ObjectMode::Unknown,
        }
    }

    /// Content-Disposition of this object
    #[napi]
    pub fn content_disposition(&self) -> Option<String> {
        self.0.content_disposition().map(|s| s.to_string())
    }

    /// Content Length of this object
    #[napi]
    pub fn content_length(&self) -> Option<u32> {
        u32::try_from(self.0.content_length()).ok()
    }

    /// Content MD5 of this object.
    #[napi]
    pub fn content_md5(&self) -> Option<String> {
        self.0.content_md5().map(|s| s.to_string())
    }

    /// Content Range of this object.
    #[napi]
    pub fn content_range(&self) -> Option<Vec<u32>> {
        let content_range = self.0
            .content_range()
            .unwrap_or_default();
        let range = content_range.range().unwrap_or_default();
        Some(vec![
            u32::try_from(range.start).ok().unwrap_or_default(),
            u32::try_from(range.end).ok().unwrap_or_default(),
            u32::try_from(content_range.size().unwrap_or_default()).ok().unwrap_or_default()
        ])
    }

    /// Content Type of this object.
    #[napi]
    pub fn content_type(&self) -> Option<String> {
        self.0.content_type().map(|s| s.to_string())
    }

    /// ETag of this object.
    #[napi]
    pub fn etag(&self) -> Option<String> {
        self.0.etag().map(|s| s.to_string())
    }

    /// Last Modified of this object.
    #[napi]
    pub fn last_modified(&self) -> Option<i64> {
        let (secs, nsecs) = self.0
            .last_modified()
            .map(|v| (v.unix_timestamp(), v.nanosecond()))
            .unwrap_or((0, 0));
        Some(DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(secs, nsecs)
                .expect("returning timestamp must be valid"),
            Utc,
        ).timestamp())
    }
}

#[napi]
pub struct ObjectLister(opendal::ObjectLister);

#[napi]
impl ObjectLister {
    #[napi]
    pub async unsafe fn next_page(&mut self) -> Result<Vec<DataObject>> {
        Ok(self.0
            .next_page()
            .await
            .map_err(format_napi_error)?.unwrap_or_default()
            .iter()
            .map(|obj| DataObject(obj.to_owned()))
            .collect())
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
    #[napi]
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
