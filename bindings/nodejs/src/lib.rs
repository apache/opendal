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
use opendal::services;
use futures::prelude::*;
use napi::bindgen_prelude::*;

#[napi]
pub struct OperatorFactory {}

#[napi]
impl OperatorFactory {
    #[napi]
    pub fn memory() -> Result<Operator> {
        let op = opendal::Operator::create(services::Memory::default())
            .unwrap()
            .finish();

        Ok(Operator::new(op))
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
pub struct Operator {
    inner: opendal::Operator
}

#[napi]
impl Operator {
    pub fn new(op: opendal::Operator) -> Self {
        Self { inner: op }
    }

    #[napi]
    pub async fn meta(&self, path: String) -> Result<ObjectMeta> {
        let o = self.inner.object(&path);
        let meta = o
            .stat()
            .await
            .map_err(format_napi_error)
            .unwrap();

        let (secs, nsecs) = meta
            .last_modified()
            .map(|v| (v.unix_timestamp(), v.nanosecond()))
            .unwrap_or((0, 0));

        Ok(ObjectMeta {
            location: path,
            last_modified: DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp_opt(secs, nsecs)
                    .expect("returning timestamp must be valid"),
                Utc,
            ).timestamp(),
            size: meta.content_length() as u32
        })
    }

    #[napi]
    pub async fn write(&self,  path: String, content: Vec<u8>) -> Result<()> {
        self.inner.object(&path)
            .write(content)
            .map_err(format_napi_error)
            .await
    }

    #[napi]
    pub async fn read(&self,  path: String) -> Result<Vec<u8>> {
        let res = self.inner.object(&path)
            .read()
            .await
            .map_err(format_napi_error)
            .unwrap();
        Ok(res)
    }

    #[napi]
    pub async fn delete(&self, path: String) -> Result<()> {
        let o = self.inner.object(&path);
        o.delete()
            .await
            .map_err(format_napi_error)
    }
}

fn format_napi_error(err: opendal::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
