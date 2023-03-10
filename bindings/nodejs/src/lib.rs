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

#[macro_use]
extern crate napi_derive;

use std::collections::HashMap;
use std::str::FromStr;

use futures::TryStreamExt;
use napi::bindgen_prelude::*;
use time::format_description::well_known::Rfc3339;

fn build_operator(
    scheme: opendal::Scheme,
    map: HashMap<String, String>,
) -> Result<opendal::Operator> {
    use opendal::services::*;

    let op = match scheme {
        opendal::Scheme::Azblob => opendal::Operator::from_map::<Azblob>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Azdfs => opendal::Operator::from_map::<Azdfs>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Fs => opendal::Operator::from_map::<Fs>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Gcs => opendal::Operator::from_map::<Gcs>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Ghac => opendal::Operator::from_map::<Ghac>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Http => opendal::Operator::from_map::<Http>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Ipmfs => opendal::Operator::from_map::<Ipmfs>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Memory => opendal::Operator::from_map::<Memory>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Obs => opendal::Operator::from_map::<Obs>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Oss => opendal::Operator::from_map::<Oss>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::S3 => opendal::Operator::from_map::<S3>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Webdav => opendal::Operator::from_map::<Webdav>(map)
            .map_err(format_napi_error)?
            .finish(),
        opendal::Scheme::Webhdfs => opendal::Operator::from_map::<Webhdfs>(map)
            .map_err(format_napi_error)?
            .finish(),
        _ => {
            return Err(format_napi_error(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "not supported scheme",
            )))
        }
    };

    Ok(op)
}

#[napi]
pub struct Operator(opendal::Operator);

#[napi]
impl Operator {
    #[napi(constructor)]
    pub fn new(scheme: String, options: Option<HashMap<String, String>>) -> Result<Self> {
        let scheme = opendal::Scheme::from_str(&scheme)
            .map_err(|err| {
                opendal::Error::new(opendal::ErrorKind::Unexpected, "not supported scheme")
                    .set_source(err)
            })
            .map_err(format_napi_error)?;
        let options = options.unwrap_or_default();

        build_operator(scheme, options).map(Operator)
    }

    #[napi]
    pub async fn stat(&self, path: String) -> Result<Metadata> {
        let meta = self.0.stat(&path).await.map_err(format_napi_error)?;

        Ok(Metadata(meta))
    }

    #[napi]
    pub fn stat_sync(&self, path: String) -> Result<Metadata> {
        let meta = self.0.blocking().stat(&path).map_err(format_napi_error)?;

        Ok(Metadata(meta))
    }

    #[napi]
    pub async fn create_dir(&self, path: String) -> Result<()> {
        self.0.create_dir(&path).await.map_err(format_napi_error)
    }

    #[napi]
    pub fn create_dir_sync(&self, path: String) -> Result<()> {
        self.0
            .blocking()
            .create_dir(&path)
            .map_err(format_napi_error)
    }

    #[napi]
    pub async fn write(&self, path: String, content: Either<Buffer, String>) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.0.write(&path, c).await.map_err(format_napi_error)
    }

    #[napi]
    pub fn write_sync(&self, path: String, content: Either<Buffer, String>) -> Result<()> {
        let c = content.as_ref().to_owned();
        self.0.blocking().write(&path, c).map_err(format_napi_error)
    }

    #[napi]
    pub async fn read(&self, path: String) -> Result<Buffer> {
        let res = self.0.read(&path).await.map_err(format_napi_error)?;
        Ok(res.into())
    }

    #[napi]
    pub fn read_sync(&self, path: String) -> Result<Buffer> {
        let res = self.0.blocking().read(&path).map_err(format_napi_error)?;
        Ok(res.into())
    }

    #[napi]
    pub async fn scan(&self, path: String) -> Result<Lister> {
        Ok(Lister(self.0.scan(&path).await.map_err(format_napi_error)?))
    }

    #[napi]
    pub fn scan_sync(&self, path: String) -> Result<BlockingLister> {
        Ok(BlockingLister(
            self.0.blocking().scan(&path).map_err(format_napi_error)?,
        ))
    }

    #[napi]
    pub async fn delete(&self, path: String) -> Result<()> {
        self.0.delete(&path).await.map_err(format_napi_error)
    }

    #[napi]
    pub fn delete_sync(&self, path: String) -> Result<()> {
        self.0.blocking().delete(&path).map_err(format_napi_error)
    }

    #[napi]
    pub async fn list(&self, path: String) -> Result<Lister> {
        Ok(Lister(self.0.list(&path).await.map_err(format_napi_error)?))
    }

    #[napi]
    pub fn list_sync(&self, path: String) -> Result<BlockingLister> {
        Ok(BlockingLister(
            self.0.blocking().scan(&path).map_err(format_napi_error)?,
        ))
    }
}

#[napi]
pub struct Entry(opendal::Entry);

#[napi]
impl Entry {
    #[napi]
    pub fn path(&self) -> String {
        self.0.path().to_string()
    }
}

#[napi]
pub struct Metadata(opendal::Metadata);

#[napi]
impl Metadata {
    /// Returns true if the <op.stat> object describes a file system directory.
    #[napi]
    pub fn is_directory(&self) -> bool {
        self.0.is_dir()
    }

    /// Returns true if the <op.stat> object describes a regular file.
    #[napi]
    pub fn is_file(&self) -> bool {
        self.0.is_file()
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
pub struct Lister(opendal::Lister);

#[napi]
impl Lister {
    /// # Safety
    ///
    /// > &mut self in async napi methods should be marked as unsafe
    ///
    /// napi will make sure the function is safe, and we didn't do unsafe
    /// thing internally.
    #[napi]
    pub async unsafe fn next(&mut self) -> Result<Option<Entry>> {
        Ok(self
            .0
            .try_next()
            .await
            .map_err(format_napi_error)?
            .map(Entry))
    }
}

#[napi]
pub struct BlockingLister(opendal::BlockingLister);

/// Method `next` can be confused for the standard trait method `std::iter::Iterator::next`.
/// But in JavaScript, it is also customary to use the next method directly to obtain the next element.
/// Therefore, disable this clippy. It can be removed after a complete implementation of Generator.
/// FYI: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Generator
#[napi]
#[allow(clippy::should_implement_trait)]
impl BlockingLister {
    #[napi]
    pub fn next(&mut self) -> Result<Option<Entry>> {
        match self.0.next() {
            Some(Ok(entry)) => Ok(Some(Entry(entry))),
            Some(Err(e)) => Err(format_napi_error(e)),
            None => Ok(None),
        }
    }
}

fn format_napi_error(err: opendal::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
