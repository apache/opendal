#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

use std::str;

use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use opendal::services;
use opendal::Operator;
use bytes::Bytes;
use futures::prelude::*;
use napi::bindgen_prelude::*;

#[napi]
pub fn debug() -> String {
    let op = Operator::create(services::Memory::default())
        .unwrap()
        .finish();
    format!("{:?}", op.metadata())
}


#[napi]
pub struct OperatorFactory {}

#[napi]
impl OperatorFactory {
    #[napi]
    pub fn memory() -> napi::Result<OpendalStore> {
        let op = Operator::create(services::Memory::default())
            .unwrap()
            .finish();

        Ok(OpendalStore::new(op))
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
pub struct OpendalStore {
    inner: Operator
}

#[napi]
impl OpendalStore {
    pub fn new(op: Operator) -> Self {
        Self { inner: op }
    }

    #[napi]
    pub fn meta(&self) -> Result<String> {
        Ok(format!("{:?}", self.inner.metadata()))
    }

    #[napi]
    pub async fn head(&self, path: String) -> Result<ObjectMeta> {
        let o = self.inner.object(&path);
        let meta = o
            .stat()
            .await
            .map_err(|err| Error::new(Status::Unknown, format!("stats get failure: {}", err.to_string())))
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
    pub async fn put(&self,  path: String, content: String) -> Result<()> {
        self.inner.object(&path)
            .write(Bytes::from(content))
            .map_err(|err|
                Error::new(Status::Unknown, format!("write failure: {}", err.to_string()))
            )
            .await
    }

    #[napi]
    pub async fn get(&self,  path: String) -> Result<String> {
        let res = self.inner.object(&path)
            .read()
            .await
            .map_err(|err|
                Error::new(Status::Unknown, format!("read failure: {}", err.to_string())))
            .unwrap();
        Ok(str::from_utf8(&res)
            .unwrap()
            .to_string())
    }

    #[napi]
    pub async fn delete(&self, path: String) -> Result<()> {
        let o = self.inner.object(&path);
        o.delete()
            .await
            .map_err(|err| Error::new(Status::Unknown, format!("delete failure: {}", err.to_string())))
    }
}
