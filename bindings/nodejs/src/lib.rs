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

#[macro_use]
extern crate napi_derive;

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use futures::TryStreamExt;
use napi::bindgen_prelude::*;

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

    /// Get current path's metadata **without cache** directly.
    ///
    /// ### Notes
    /// Use stat if you:
    ///
    /// - Want detect the outside changes of path.
    /// - Don’t want to read from cached metadata.
    ///
    /// You may want to use `metadata` if you are working with entries returned by `Lister`. It’s highly possible that metadata you want has already been cached.
    ///
    /// ### Example
    /// ```javascript
    /// const meta = await op.stat("test");
    /// if (meta.isDir) {
    ///   // do something
    /// }
    /// ```
    #[napi]
    pub async fn stat(&self, path: String) -> Result<Metadata> {
        let meta = self.0.stat(&path).await.map_err(format_napi_error)?;

        Ok(Metadata(meta))
    }

    /// Get current path's metadata **without cache** directly and synchronously.
    ///
    /// ### Example
    /// ```javascript
    /// const meta = op.statSync("test");
    /// if (meta.isDir) {
    ///   // do something
    /// }
    /// ```
    #[napi]
    pub fn stat_sync(&self, path: String) -> Result<Metadata> {
        let meta = self.0.blocking().stat(&path).map_err(format_napi_error)?;

        Ok(Metadata(meta))
    }

    /// Check if this operator can work correctly.
    ///
    /// We will send a `list` request to path and return any errors we met.
    ///
    /// ### Example
    /// ```javascript
    /// await op.check();
    /// ```
    #[napi]
    pub async fn check(&self) -> Result<()> {
        self.0.check().await.map_err(format_napi_error)
    }

    /// Check if this path exists or not.
    ///
    /// ### Example
    /// ```javascript
    /// await op.isExist("test");
    /// ```
    #[napi]
    pub async fn is_exist(&self, path: String) -> Result<bool> {
        self.0.is_exist(&path).await.map_err(format_napi_error)
    }

    /// Check if this path exists or not synchronously.
    ///
    /// ### Example
    /// ```javascript
    /// op.isExistSync("test");
    /// ```
    #[napi]
    pub fn is_exist_sync(&self, path: String) -> Result<bool> {
        self.0.blocking().is_exist(&path).map_err(format_napi_error)
    }

    /// Create dir with given path.
    ///
    /// ### Example
    /// ```javascript
    /// await op.createDir("path/to/dir/");
    /// ```
    #[napi]
    pub async fn create_dir(&self, path: String) -> Result<()> {
        self.0.create_dir(&path).await.map_err(format_napi_error)
    }

    /// Create dir with given path synchronously.
    ///
    /// ### Example
    /// ```javascript
    /// op.createDirSync("path/to/dir/");
    /// ```
    #[napi]
    pub fn create_dir_sync(&self, path: String) -> Result<()> {
        self.0
            .blocking()
            .create_dir(&path)
            .map_err(format_napi_error)
    }

    /// Read the whole path into a buffer.
    ///
    /// ### Example
    /// ```javascript
    /// const buf = await op.read("path/to/file");
    /// ```
    #[napi]
    pub async fn read(&self, path: String) -> Result<Buffer> {
        let res = self.0.read(&path).await.map_err(format_napi_error)?;
        Ok(res.into())
    }

    /// Read the whole path into a buffer synchronously.
    ///
    /// ### Example
    /// ```javascript
    /// const buf = op.readSync("path/to/file");
    /// ```
    #[napi]
    pub fn read_sync(&self, path: String) -> Result<Buffer> {
        let res = self.0.blocking().read(&path).map_err(format_napi_error)?;
        Ok(res.into())
    }

    /// Write bytes into path.
    ///
    /// ### Example
    /// ```javascript
    /// await op.write("path/to/file", Buffer.from("hello world"));
    /// // or
    /// await op.write("path/to/file", "hello world");
    /// ```
    #[napi]
    pub async fn write(&self, path: String, content: Either<Buffer, String>) -> Result<()> {
        let c = match content {
            Either::A(buf) => buf.as_ref().to_owned(),
            Either::B(s) => s.into_bytes(),
        };
        self.0.write(&path, c).await.map_err(format_napi_error)
    }

    /// Write bytes into path synchronously.
    ///
    /// ### Example
    /// ```javascript
    /// op.writeSync("path/to/file", Buffer.from("hello world"));
    /// // or
    /// op.writeSync("path/to/file", "hello world");
    /// ```
    #[napi]
    pub fn write_sync(&self, path: String, content: Either<Buffer, String>) -> Result<()> {
        let c = match content {
            Either::A(buf) => buf.as_ref().to_owned(),
            Either::B(s) => s.into_bytes(),
        };
        self.0.blocking().write(&path, c).map_err(format_napi_error)
    }

    /// Copy file according to given `from` and `to` path.
    ///
    /// ### Example
    /// ```javascript
    /// await op.copy("path/to/file", "path/to/dest");
    /// ```
    #[napi]
    pub async fn copy(&self, from: String, to: String) -> Result<()> {
        self.0.copy(&from, &to).await.map_err(format_napi_error)
    }

    /// Copy file according to given `from` and `to` path synchronously.
    ///
    /// ### Example
    /// ```javascript
    /// op.copySync("path/to/file", "path/to/dest");
    /// ```
    #[napi]
    pub fn copy_sync(&self, from: String, to: String) -> Result<()> {
        self.0
            .blocking()
            .copy(&from, &to)
            .map_err(format_napi_error)
    }

    /// Rename file according to given `from` and `to` path.
    ///
    /// It's similar to `mv` command.
    ///
    /// ### Example
    /// ```javascript
    /// await op.rename("path/to/file", "path/to/dest");
    /// ```
    #[napi]
    pub async fn rename(&self, from: String, to: String) -> Result<()> {
        self.0.rename(&from, &to).await.map_err(format_napi_error)
    }

    /// Rename file according to given `from` and `to` path synchronously.
    ///
    /// It's similar to `mv` command.
    ///
    /// ### Example
    /// ```javascript
    /// op.renameSync("path/to/file", "path/to/dest");
    /// ```
    #[napi]
    pub fn rename_sync(&self, from: String, to: String) -> Result<()> {
        self.0
            .blocking()
            .rename(&from, &to)
            .map_err(format_napi_error)
    }

    /// List dir in flat way.
    ///
    /// This function will create a new handle to list entries.
    ///
    /// An error will be returned if given path doesn’t end with /.
    ///
    /// ### Example
    /// ```javascript
    /// const lister = await op.scan("/path/to/dir/");
    /// while (true) {
    ///   const entry = await lister.next();
    ///   if (entry === null) {
    ///     break;
    ///   }
    ///   let meta = await op.stat(entry.path);
    ///   if (meta.is_file) {
    ///     // do something
    ///   }
    /// }
    /// `````
    #[napi]
    pub async fn scan(&self, path: String) -> Result<Lister> {
        Ok(Lister(self.0.scan(&path).await.map_err(format_napi_error)?))
    }

    /// List dir in flat way synchronously.
    ///
    /// This function will create a new handle to list entries.
    ///
    /// An error will be returned if given path doesn’t end with /.
    ///
    /// ### Example
    /// ```javascript
    /// const lister = op.scan_sync(/path/to/dir/");
    /// while (true) {
    ///   const entry = lister.next();
    ///   if (entry === null) {
    ///     break;
    ///   }
    ///   let meta = op.statSync(entry.path);
    ///   if (meta.is_file) {
    ///     // do something
    ///   }
    /// }
    /// `````
    #[napi]
    pub fn scan_sync(&self, path: String) -> Result<BlockingLister> {
        Ok(BlockingLister(
            self.0.blocking().scan(&path).map_err(format_napi_error)?,
        ))
    }

    /// Delete the given path.
    ///
    /// ### Notes
    /// Delete not existing error won’t return errors.
    ///
    /// ### Example
    /// ```javascript
    /// await op.delete("test");
    /// ```
    #[napi]
    pub async fn delete(&self, path: String) -> Result<()> {
        self.0.delete(&path).await.map_err(format_napi_error)
    }

    /// Delete the given path synchronously.
    ///
    /// ### Example
    /// ```javascript
    /// op.deleteSync("test");
    /// ```
    #[napi]
    pub fn delete_sync(&self, path: String) -> Result<()> {
        self.0.blocking().delete(&path).map_err(format_napi_error)
    }

    /// Remove given paths.
    ///
    /// ### Notes
    /// If underlying services support delete in batch, we will use batch delete instead.
    ///
    /// ### Examples
    /// ```javascript
    /// await op.remove(["abc", "def"]);
    /// ```
    #[napi]
    pub async fn remove(&self, paths: Vec<String>) -> Result<()> {
        self.0.remove(paths).await.map_err(format_napi_error)
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// ### Notes
    /// If underlying services support delete in batch, we will use batch delete instead.
    ///
    /// ### Examples
    /// ```javascript
    /// await op.removeAll("path/to/dir/");
    /// ```
    #[napi]
    pub async fn remove_all(&self, path: String) -> Result<()> {
        self.0.remove_all(&path).await.map_err(format_napi_error)
    }

    /// List given path.
    ///
    /// This function will create a new handle to list entries.
    ///
    /// An error will be returned if given path doesn't end with `/`.
    ///
    /// ### Example
    /// ```javascript
    /// const lister = await op.list("path/to/dir/");
    /// while (true) {
    ///   const entry = await lister.next();
    ///   if (entry === null) {
    ///     break;
    ///   }
    ///   let meta = await op.stat(entry.path);
    ///   if (meta.isFile) {
    ///     // do something
    ///   }
    /// }
    /// ```
    #[napi]
    pub async fn list(&self, path: String) -> Result<Lister> {
        Ok(Lister(self.0.list(&path).await.map_err(format_napi_error)?))
    }

    /// List given path synchronously.
    ///
    /// This function will create a new handle to list entries.
    ///
    /// An error will be returned if given path doesn't end with `/`.
    ///
    /// ### Example
    /// ```javascript
    /// const lister = op.listSync("path/to/dir/");
    /// while (true) {
    ///   const entry = lister.next();
    ///   if (entry === null) {
    ///     break;
    ///   }
    ///   let meta = op.statSync(entry.path);
    ///   if (meta.isFile) {
    ///     // do something
    ///   }
    /// }
    /// ```
    #[napi]
    pub fn list_sync(&self, path: String) -> Result<BlockingLister> {
        Ok(BlockingLister(
            self.0.blocking().scan(&path).map_err(format_napi_error)?,
        ))
    }

    /// Get a presigned request for read.
    ///
    /// Unit of expires is seconds.
    ///
    /// ### Example
    ///
    /// ```javascript
    /// const req = await op.presignRead(path, parseInt(expires));
    ///
    /// console.log("method: ", req.method);
    /// console.log("url: ", req.url);
    /// console.log("headers: ", req.headers);
    /// ```
    #[napi]
    pub async fn presign_read(&self, path: String, expires: u32) -> Result<PresignedRequest> {
        let res = self
            .0
            .presign_read(&path, Duration::from_secs(expires as u64))
            .await
            .map_err(format_napi_error)?;
        Ok(PresignedRequest::new(res))
    }

    /// Get a presigned request for write.
    ///
    /// Unit of expires is seconds.
    ///
    /// ### Example
    ///
    /// ```javascript
    /// const req = await op.presignWrite(path, parseInt(expires));
    ///
    /// console.log("method: ", req.method);
    /// console.log("url: ", req.url);
    /// console.log("headers: ", req.headers);
    /// ```
    #[napi]
    pub async fn presign_write(&self, path: String, expires: u32) -> Result<PresignedRequest> {
        let res = self
            .0
            .presign_write(&path, Duration::from_secs(expires as u64))
            .await
            .map_err(format_napi_error)?;
        Ok(PresignedRequest::new(res))
    }

    /// Get a presigned request for stat.
    ///
    /// Unit of expires is seconds.
    ///
    /// ### Example
    ///
    /// ```javascript
    /// const req = await op.presignStat(path, parseInt(expires));
    ///
    /// console.log("method: ", req.method);
    /// console.log("url: ", req.url);
    /// console.log("headers: ", req.headers);
    /// ```
    #[napi]
    pub async fn presign_stat(&self, path: String, expires: u32) -> Result<PresignedRequest> {
        let res = self
            .0
            .presign_stat(&path, Duration::from_secs(expires as u64))
            .await
            .map_err(format_napi_error)?;
        Ok(PresignedRequest::new(res))
    }
}

#[napi]
pub struct Entry(opendal::Entry);

#[napi]
impl Entry {
    /// Return the path of this entry.
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

    /// Last Modified of this object.
    ///
    /// We will output this time in RFC3339 format like `1996-12-19T16:39:57+08:00`.
    #[napi(getter)]
    pub fn last_modified(&self) -> Option<String> {
        self.0.last_modified().map(|ta| ta.to_rfc3339())
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
/// FYI: <https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Generator>
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

#[napi(object)]
pub struct PresignedRequest {
    /// HTTP method of this request.
    pub method: String,
    /// URL of this request.
    pub url: String,
    /// HTTP headers of this request.
    pub headers: HashMap<String, String>,
}

impl PresignedRequest {
    pub fn new(req: opendal::raw::PresignedRequest) -> Self {
        let method = req.method().to_string();
        let url = req.uri().to_string();
        let headers = req
            .header()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    v.to_str()
                        .expect("header value contains non visible ascii characters")
                        .to_string(),
                )
            })
            .collect();
        Self {
            method,
            url,
            headers,
        }
    }
}

fn format_napi_error(err: opendal::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
