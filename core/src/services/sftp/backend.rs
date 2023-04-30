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

use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use async_trait::async_trait;
use bb8::PooledConnection;
use futures::executor::block_on;
use log::debug;
use openssh::RemoteChild;
use openssh::Session;
use openssh::SessionBuilder;
use openssh::Stdio;
use openssh_sftp_client::Sftp;
use owning_ref::OwningHandle;
use tokio::sync::OnceCell;

use super::error::SftpError;
use super::pager::SftpPager;
use super::utils::SftpReader;
use super::writer::SftpWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// SFTP services support. (only works on unix)
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [ ] ~~scan~~
/// - [ ] ~~presign~~
/// - [ ] blocking
///
/// # Configuration
///
/// - `endpoint`: Set the endpoint for connection
/// - `root`: Set the work directory for backend
/// - `user`: Set the login user
/// - `key`: Set the public key for login
///
/// It doesn't support password login, you can use public key instead.
///
/// You can refer to [`SftpBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Ftp;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Sftp::default();
///
///     builder.endpoint("127.0.0.1").user("test").password("test");
///
///     let op: Operator = Operator::new(builder)?.finish();
///     let _obj: Object = op.object("test_file");
///     Ok(())
/// }
/// ```

#[derive(Default)]
pub struct SftpBuilder {
    endpoint: Option<String>,
    root: Option<String>,
    user: Option<String>,
    key: Option<String>,
}

impl Debug for SftpBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish()
    }
}

impl SftpBuilder {
    /// set endpoint for sftp backend.
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };

        self
    }

    /// set root path for sftp backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// set user for sftp backend.
    pub fn user(&mut self, user: &str) -> &mut Self {
        self.user = if user.is_empty() {
            None
        } else {
            Some(user.to_string())
        };

        self
    }

    /// set key path for sftp backend.
    pub fn key(&mut self, key: &str) -> &mut Self {
        self.key = if key.is_empty() {
            None
        } else {
            Some(key.to_string())
        };

        self
    }
}

impl Builder for SftpBuilder {
    const SCHEME: Scheme = Scheme::Sftp;
    type Accessor = SftpBackend;

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("sftp backend build started: {:?}", &self);
        let endpoint = match self.endpoint.clone() {
            None => return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")),
            Some(v) => v,
        };

        let root = normalize_root(&self.root.clone().unwrap_or_default());

        let user = match &self.user {
            None => "".to_string(),
            Some(v) => v.clone(),
        };

        debug!("sftp backend finished: {:?}", &self);

        Ok(SftpBackend {
            endpoint,
            root,
            user,
            key: self.key.clone(),
            sftp: OnceCell::new(),
        })
    }

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = SftpBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("user").map(|v| builder.user(v));
        map.get("key").map(|v| builder.key(v));

        builder
    }
}

#[derive(Clone)]
pub struct Manager {
    endpoint: String,
    user: String,
    key: Option<String>,
}

pub struct Connection {
    // the remote child owns the ref to session, so we need to use owning handle
    // safety explanation will be talked about deeply in the future
    // todo: add safety explanation
    // Related: https://stackoverflow.com/a/47260399
    child: OwningHandle<Box<Session>, Box<RemoteChild<'static>>>,
    pub sftp: Sftp,
}

#[async_trait]
impl bb8::ManageConnection for Manager {
    type Connection = Connection;
    type Error = SftpError;

    async fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        let mut session = SessionBuilder::default();

        session.user(self.user.clone());

        if let Some(key) = &self.key {
            session.keyfile(key);
        }

        let session = session.connect(self.endpoint.clone()).await?;

        let sess = Box::new(session);
        let mut oref = OwningHandle::new_with_fn(sess, unsafe {
            |x| {
                Box::new(
                    block_on(
                        (*x).subsystem("sftp")
                            .stdin(Stdio::piped())
                            .stdout(Stdio::piped())
                            .spawn(),
                    )
                    .unwrap(),
                )
            }
        });

        let sftp = Sftp::new(
            oref.stdin().take().unwrap(),
            oref.stdout().take().unwrap(),
            Default::default(),
        )
        .await?;

        Ok(Connection { child: oref, sftp })
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.child.session().check().await?;
        Ok(())
    }

    /// Always allow reuse conn.
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

/// Backend is used to serve `Accessor` support for sftp.
pub struct SftpBackend {
    endpoint: String,
    root: String,
    user: String,
    key: Option<String>,
    sftp: OnceCell<bb8::Pool<Manager>>,
}

impl Debug for SftpBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend").finish()
    }
}

#[async_trait]
impl Accessor for SftpBackend {
    type Reader = SftpReader;
    type BlockingReader = ();
    type Writer = SftpWriter;
    type BlockingWriter = ();
    type Pager = SftpPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Sftp)
            .set_root(&self.root)
            .set_capability(Capability {
                stat: true,
                read: true,
                write: true,
                list: true,
                list_with_limit: true,
                
                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let client = self.sftp_connect().await?;

        let mut fs = client.sftp.fs();
        fs.set_cwd(self.root.clone());

        fs.create_dir(path).await?;

        return Ok(RpCreate::default());
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let client = self.sftp_connect().await?;

        let mut fs = client.sftp.fs();
        fs.set_cwd(self.root.clone());
        let path = fs.canonicalize(path).await?;

        let mut file = client.sftp.open(path.as_path()).await?;

        let total_length = file.metadata().await?.len().ok_or(Error::new(
            ErrorKind::NotFound,
            format!("file not found: {}", path.to_str().unwrap()).as_str(),
        ))?;

        let br = args.range();
        let (start, end) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => (offset, min(offset + size, total_length)),
            // Read from offset.
            (Some(offset), None) => (offset, total_length),
            // Read the last size bytes.
            (None, Some(size)) => (
                if total_length > size {
                    total_length - size
                } else {
                    0
                },
                total_length,
            ),
            // Read the whole file.
            (None, None) => (0, total_length),
        };

        let r = SftpReader::new(self.sftp_connect().await?, path, start, end);

        Ok((RpRead::new(end - start), r))
    }

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::new(),
            SftpWriter::new(
                self.sftp_connect().await?,
                path.to_owned(),
                self.root.clone(),
            ),
        ))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let client = self.sftp_connect().await?;

        let mut fs = client.sftp.fs();
        fs.set_cwd(self.root.clone());

        let meta = fs.metadata(path).await?;

        Ok(RpStat::new(meta.into()))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let client = self.sftp_connect().await?;

        let mut fs = client.sftp.fs();
        fs.set_cwd(self.root.clone());

        if path.ends_with('/') {
            fs.remove_dir(path).await?
        } else {
            fs.remove_file(path).await?
        };

        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let client = self.sftp_connect().await?;

        let mut fs = client.sftp.fs();
        fs.set_cwd(self.root.clone());

        let dir = fs.open_dir(path).await?.read_dir().await?;

        Ok((
            RpList::default(),
            SftpPager::new(dir, args.limit()),
        ))
    }
}

impl SftpBackend {
    pub async fn sftp_connect(&self) -> Result<PooledConnection<'static, Manager>> {
        let pool = self
            .sftp
            .get_or_try_init(|| async {
                let manager = Manager {
                    endpoint: self.endpoint.clone(),
                    user: self.user.clone(),
                    key: self.key.clone(),
                };

                bb8::Pool::builder().max_size(10).build(manager).await
            })
            .await?;

        let conn = pool.get_owned().await?;

        Ok(conn)
    }
}
