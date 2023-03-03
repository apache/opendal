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
use opendal::Builder;

#[napi]
pub struct OperatorFactory {}

#[napi]
impl OperatorFactory {
    #[napi]
    pub fn memory() -> Result<Operator> {
        let op = opendal::Operator::create(opendal::services::Memory::default())
            .unwrap()
            .finish();

        Ok(Operator::new(op))
    }
}

pub struct BaseBuilder<B: opendal::Builder> {
    inner: B
}
//
// #[napi]
// pub struct MemoryBuilder = BaseBuilder<opendal::services::Memory>
//
// #[napi]
// impl MemoryBuilder {
//     #[napi(constructor)]
//     pub fn new() -> Self {
//         MemoryBuilder{
//             inner: opendal::services::Memory::default()
//         }
//     }
// }

#[napi]
pub struct Operator {
    inner: opendal::Operator
}

#[napi]
impl Operator {
    pub fn new(op: opendal::Operator) -> Self {
        Self { inner: op }
    }
    // pub fn new<T>(op: opendal::OperatorBuilder<T>) -> Self {
    //     Self { inner: op }
    // }

    // #[napi]
    // pub fn create -> Self {
    //     Buffer
    // }


    #[napi]
    pub fn object(&self, path: String) -> Object {
        Object::new(self.inner.object(&path))
    }
}


#[allow(dead_code)]
#[napi]
pub struct ObjectMetadata {
    pub location: String,
    pub last_modified: i64,
    pub size: u32
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
    pub async fn meta(&self) -> Result<ObjectMetadata> {
        let meta = self.inner
            .stat()
            .await
            .map_err(format_napi_error)
            .unwrap();

        let (secs, nsecs) = meta
            .last_modified()
            .map(|v| (v.unix_timestamp(), v.nanosecond()))
            .unwrap_or((0, 0));

        Ok(ObjectMetadata {
            location: self.inner.path().to_string(),
            last_modified: DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp_opt(secs, nsecs)
                    .expect("returning timestamp must be valid"),
                Utc,
            ).timestamp(),
            size: meta.content_length() as u32
        })
    }

    #[napi]
    pub async fn write(&self, content: Buffer) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.inner
            .write(c)
            .await
            .map_err(format_napi_error)
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

    #[napi]
    pub async fn delete(&self) -> Result<()> {
        self.inner
            .delete()
            .await
            .map_err(format_napi_error)
    }
}

fn format_napi_error(err: opendal::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
