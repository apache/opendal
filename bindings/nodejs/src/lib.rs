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
        Operator {
            inner: opendal::Operator::create(opendal::services::Memory::default()).unwrap().finish()
        }
    }
}

#[napi]
pub struct Operator {
    inner: opendal::Operator
}

#[napi]
impl Operator {
    pub fn new(op: opendal::Operator) -> Self {
        Self { inner: op }
    }

    #[napi]
    pub fn object(&self, path: String) -> Object {
        Object::new(self.inner.object(&path))
    }
}

#[allow(dead_code)]
#[napi]
pub struct ObjectMeta {
    pub location: String,
    pub last_modified: i64,
    pub size: u32
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
pub struct ObjectMetadata {
    /// Mode of this object.
    pub mode: ObjectMode,

    /// Content-Disposition of this object
    pub content_disposition: Option<String>,
    /// Content Length of this object
    pub content_length: Option<u32>,
    /// Content MD5 of this object.
    pub content_md5: Option<String>,
    /// Content Range of this object.
    pub content_range: Option<Vec<u32>>,
    /// Content Type of this object.
    pub content_type: Option<String>,
    /// ETag of this object.
    pub etag: Option<String>,
    /// Last Modified of this object.
    pub last_modified: i64,
}

#[napi]
pub struct Object {
    inner: opendal::Object
}

#[napi]
impl Object {
    pub fn new(op: opendal::Object) -> Self {
        Self { inner: op }
    }

    #[napi]
    pub async fn stat(&self) -> Result<ObjectMetadata> {
        let meta = self.inner
            .stat()
            .await
            .map_err(format_napi_error)
            .unwrap();

        exact_meta(meta)
    }

    #[napi(js_name="statSync")]
    pub fn blocking_stat(&self) -> Result<ObjectMetadata> {
        let meta = self.inner
            .blocking_stat()
            .map_err(format_napi_error)
            .unwrap();

        exact_meta(meta)
    }

    #[napi]
    pub async fn write(&self, content: Buffer) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.inner
            .write(c)
            .await
            .map_err(format_napi_error)
    }

    #[napi(js_name="writeSync")]
    pub fn blocking_write(&self, content: Buffer) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.inner.blocking_write(c).map_err(format_napi_error)
    }

    #[napi]
    pub async fn read(&self) -> Result<Buffer> {
        let res = self.inner
            .read()
            .await
            .map_err(format_napi_error)
            .unwrap();
        Ok(res.into())
    }

    #[napi(js_name="readSync")]
    #[napi]
    pub fn blocking_read(&self) -> Result<Buffer> {
        let res = self.inner
            .blocking_read()
            .map_err(format_napi_error)
            .unwrap();
        Ok(res.into())
    }

    #[napi]
    pub async fn delete(&self) -> Result<()> {
        self.inner
            .delete()
            .await
            .map_err(format_napi_error)
    }

    #[napi(js_name="deleteSync")]
    pub fn blocking_delete(&self) -> Result<()> {
        self.inner
            .blocking_delete()
            .map_err(format_napi_error)
    }
}

fn exact_meta(meta: opendal::ObjectMetadata) -> Result<ObjectMetadata> {
    let content_range = meta
        .content_range()
        .unwrap_or_default();
    let range = content_range.range().unwrap_or_default();
    let range_out: Vec<u32> = vec![
        u32::try_from(range.start).ok().unwrap_or_default(),
        u32::try_from(range.end).ok().unwrap_or_default(),
        u32::try_from(content_range.size().unwrap_or_default()).ok().unwrap_or_default()
    ];

    let (secs, nsecs) = meta
        .last_modified()
        .map(|v| (v.unix_timestamp(), v.nanosecond()))
        .unwrap_or((0, 0));


    Ok(ObjectMetadata {
        mode: match meta.mode() {
            opendal::ObjectMode::DIR => ObjectMode::DIR,
            opendal::ObjectMode::FILE => ObjectMode::FILE,
            opendal::ObjectMode::Unknown => ObjectMode::Unknown,
        },
        content_disposition: meta.content_disposition().map(|s| s.to_string()),
        content_length: u32::try_from(meta.content_length()).ok(),
        content_md5: meta.content_md5().map(|s| s.to_string()),
        content_range: Some(range_out),
        content_type: meta.content_type().map(|s| s.to_string()),
        etag: meta.etag().map(|s| s.to_string()),
        last_modified: DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(secs, nsecs)
                .expect("returning timestamp must be valid"),
            Utc,
        ).timestamp(),
    })
}

fn format_napi_error(err: opendal::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
