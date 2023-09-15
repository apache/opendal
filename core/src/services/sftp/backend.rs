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
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use openssh::KnownHosts;
use openssh::SessionBuilder;
use openssh_sftp_client::Sftp;
use openssh_sftp_client::SftpOptions;

use super::error::is_not_found;
use super::error::is_sftp_protocol_error;
use super::pager::SftpPager;
use super::utils::SftpReader;
use super::writer::SftpWriter;
use crate::raw::*;
use crate::*;

/// SFTP services support. (only works on unix)
///
/// If you are interested in working on windows, please refer to [this](https://github.com/apache/incubator-opendal/issues/2963) issue.
/// Welcome to leave your comments or make contributions.
///
/// Warning: Maximum number of file holdings is depending on the remote system configuration.
///
/// For example, the default value is 255 in macOS, and 1024 in linux. If you want to open
/// lots of files, you should pay attention to close the file after using it.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct SftpBuilder {
    endpoint: Option<String>,
    root: Option<String>,
    user: Option<String>,
    key: Option<String>,
    known_hosts_strategy: Option<String>,
    enable_copy: bool,
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
    /// It uses the default directory set by the remote `sftp-server` as default.
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

    /// set known_hosts strategy for sftp backend.
    /// available values:
    /// - Strict (default)
    /// - Accept
    /// - Add
    pub fn known_hosts_strategy(&mut self, strategy: &str) -> &mut Self {
        self.known_hosts_strategy = if strategy.is_empty() {
            None
        } else {
            Some(strategy.to_string())
        };

        self
    }

    /// set enable_copy for sftp backend.
    /// It requires the server supports copy-file extension.
    pub fn enable_copy(&mut self, enable_copy: bool) -> &mut Self {
        self.enable_copy = enable_copy;

        self
    }
}

impl Builder for SftpBuilder {
    const SCHEME: Scheme = Scheme::Sftp;
    type Accessor = SftpBackend;

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("sftp backend build started: {:?}", &self);
        let endpoint = match self.endpoint.clone() {
            Some(v) => v,
            None => return Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")),
        };

        let user = match self.user.clone() {
            Some(v) => v,
            None => return Err(Error::new(ErrorKind::ConfigInvalid, "user is empty")),
        };

        let root = self
            .root
            .clone()
            .map(|r| normalize_root(r.as_str()))
            .unwrap_or_default();

        let known_hosts_strategy = match &self.known_hosts_strategy {
            Some(v) => {
                let v = v.to_lowercase();
                if v == "strict" {
                    KnownHosts::Strict
                } else if v == "accept" {
                    KnownHosts::Accept
                } else if v == "add" {
                    KnownHosts::Add
                } else {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        format!("unknown known_hosts strategy: {}", v).as_str(),
                    ));
                }
            }
            None => KnownHosts::Strict,
        };

        debug!("sftp backend finished: {:?}", &self);

        Ok(SftpBackend {
            endpoint,
            root,
            user,
            key: self.key.clone(),
            known_hosts_strategy,
            copyable: self.enable_copy,
            client: tokio::sync::OnceCell::new(),
        })
    }

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = SftpBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("user").map(|v| builder.user(v));
        map.get("key").map(|v| builder.key(v));
        map.get("known_hosts_strategy")
            .map(|v| builder.known_hosts_strategy(v));

        builder
    }
}

/// Backend is used to serve `Accessor` support for sftp.
pub struct SftpBackend {
    endpoint: String,
    root: String,
    user: String,
    key: Option<String>,
    known_hosts_strategy: KnownHosts,
    copyable: bool,
    client: tokio::sync::OnceCell<Sftp>,
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
    type Pager = Option<SftpPager>;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_root(self.root.as_str())
            .set_scheme(Scheme::Sftp)
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_with_range: true,
                read_can_seek: true,

                write: true,
                write_can_multi: true,

                create_dir: true,
                delete: true,

                list: true,
                list_with_limit: true,
                list_with_delimiter_slash: true,

                copy: self.copyable,
                rename: true,

                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let client = self.connect().await?;
        let mut fs = client.fs();
        fs.set_cwd(&self.root);

        let paths = Path::new(&path).components();
        let mut current = PathBuf::from(&self.root);
        for p in paths {
            current = current.join(p);
            let res = fs.create_dir(p).await;

            if let Err(e) = res {
                // ignore error if dir already exists
                if !is_sftp_protocol_error(&e) {
                    return Err(e.into());
                }
            }
            fs.set_cwd(&current);
        }

        return Ok(RpCreateDir::default());
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let client = self.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.root);
        let path = fs.canonicalize(path).await?;

        let mut file = client.open(path.as_path()).await?;

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

        let r = SftpReader::new(file, start, end).await?;

        Ok((RpRead::new(end - start), r))
    }

    async fn write(&self, path: &str, op: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if let Some((dir, _)) = path.rsplit_once('/') {
            self.create_dir(dir, OpCreateDir::default()).await?;
        }

        let client = self.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.root);
        let path = fs.canonicalize(path).await?;

        let mut option = client.options();
        option.create(true);
        if op.append() {
            option.append(true);
        } else {
            option.write(true);
        }

        let file = option.open(path).await?;

        Ok((RpWrite::new(), SftpWriter::new(file)))
    }

    async fn copy(&self, from: &str, to: &str, _: OpCopy) -> Result<RpCopy> {
        let client = self.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.root);

        if let Some((dir, _)) = to.rsplit_once('/') {
            self.create_dir(dir, OpCreateDir::default()).await?;
        }

        let src = fs.canonicalize(from).await?;
        let dst = fs.canonicalize(to).await?;
        let mut src_file = client.open(&src).await?;
        let mut dst_file = client.create(dst).await?;

        src_file.copy_all_to(&mut dst_file).await?;

        Ok(RpCopy::default())
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        let client = self.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.root);

        if let Some((dir, _)) = to.rsplit_once('/') {
            self.create_dir(dir, OpCreateDir::default()).await?;
        }
        fs.rename(from, to).await?;

        Ok(RpRename::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let client = self.connect().await?;
        let mut fs = client.fs();
        fs.set_cwd(&self.root);

        let meta = fs.metadata(path).await?;

        Ok(RpStat::new(meta.into()))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let client = self.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.root);

        if path.ends_with('/') {
            let file_path = format!("./{}", path);
            let mut dir = match fs.open_dir(&file_path).await {
                Ok(dir) => dir,
                Err(e) => {
                    if is_not_found(&e) {
                        return Ok(RpDelete::default());
                    } else {
                        return Err(e.into());
                    }
                }
            }
            .read_dir()
            .boxed();

            while let Some(file) = dir.next().await {
                let file = file?;
                let file_name = file.filename().to_str();
                if file_name == Some(".") || file_name == Some("..") {
                    continue;
                }
                let file_path = Path::new(&self.root).join(file.filename());
                self.delete(
                    file_path.to_str().ok_or(Error::new(
                        ErrorKind::Unexpected,
                        "unable to convert file path to str",
                    ))?,
                    OpDelete::default(),
                )
                .await?;
            }

            match fs.remove_dir(path).await {
                Err(e) if !is_not_found(&e) => {
                    return Err(e.into());
                }
                _ => {}
            }
        } else {
            match fs.remove_file(path).await {
                Err(e) if !is_not_found(&e) => {
                    return Err(e.into());
                }
                _ => {}
            }
        };

        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        let client = self.connect().await?;
        let mut fs = client.fs();
        fs.set_cwd(&self.root);

        let file_path = format!("./{}", path);

        let dir = match fs.open_dir(&file_path).await {
            Ok(dir) => dir,
            Err(e) => {
                if is_not_found(&e) {
                    return Ok((RpList::default(), None));
                } else {
                    return Err(e.into());
                }
            }
        }
        .read_dir();

        Ok((
            RpList::default(),
            Some(SftpPager::new(dir, path.to_owned(), args.limit())),
        ))
    }
}

impl SftpBackend {
    async fn connect(&self) -> Result<&Sftp> {
        let sftp = self
            .client
            .get_or_try_init(|| {
                Box::pin(connect_sftp(
                    self.endpoint.as_str(),
                    self.root.clone(),
                    self.user.clone(),
                    self.key.clone(),
                    self.known_hosts_strategy.clone(),
                ))
            })
            .await?;

        Ok(sftp)
    }
}

async fn connect_sftp(
    endpoint: &str,
    root: String,
    user: String,
    key: Option<String>,
    known_hosts_strategy: KnownHosts,
) -> Result<Sftp> {
    let mut session = SessionBuilder::default();

    session.user(user);

    if let Some(key) = &key {
        session.keyfile(key);
    }

    // set control directory to avoid temp files in root directory when panic
    if let Some(dir) = dirs::runtime_dir() {
        session.control_directory(dir);
    }

    #[cfg(target_os = "macos")]
    {
        let _ = std::fs::create_dir("/private/tmp/.opendal/");
        session.control_directory("/private/tmp/.opendal/");
    }

    session.known_hosts_check(known_hosts_strategy);

    let session = session.connect(&endpoint).await?;

    let sftp = Sftp::from_session(session, SftpOptions::default()).await?;

    if !root.is_empty() {
        let mut fs = sftp.fs();

        let paths = Path::new(&root).components();
        let mut current = PathBuf::new();
        for p in paths {
            current.push(p);
            let res = fs.create_dir(p).await;

            if let Err(e) = res {
                // ignore error if dir already exists
                if !is_sftp_protocol_error(&e) {
                    return Err(e.into());
                }
            }
            fs.set_cwd(&current);
        }
    }

    debug!("sftp connection created at {}", root);

    Ok(sftp)
}
