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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use bb8::PooledConnection;
use bb8::RunError;
use log::debug;
use openssh::KnownHosts;
use openssh::SessionBuilder;
use openssh_sftp_client::Sftp;
use openssh_sftp_client::SftpOptions;
use tokio::sync::OnceCell;

use super::error::is_sftp_protocol_error;
use super::error::parse_sftp_error;
use super::error::parse_ssh_error;
use crate::raw::*;
use crate::*;

pub struct SftpCore {
    pub info: Arc<AccessorInfo>,
    pub endpoint: String,
    pub root: String,
    pub user: Option<String>,
    pub key: Option<String>,
    pub known_hosts_strategy: KnownHosts,

    pub client: OnceCell<bb8::Pool<Manager>>,
}

impl Debug for SftpCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpCore")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish()
    }
}

impl SftpCore {
    pub async fn connect(&self) -> Result<PooledConnection<'static, Manager>> {
        let client = self
            .client
            .get_or_try_init(|| async {
                bb8::Pool::builder()
                    .max_size(64)
                    .build(Manager {
                        endpoint: self.endpoint.clone(),
                        root: self.root.clone(),
                        user: self.user.clone(),
                        key: self.key.clone(),
                        known_hosts_strategy: self.known_hosts_strategy.clone(),
                    })
                    .await
            })
            .await?;

        client.get_owned().await.map_err(|err| match err {
            RunError::User(err) => err,
            RunError::TimedOut => {
                Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary()
            }
        })
    }
}

pub struct Manager {
    endpoint: String,
    root: String,
    user: Option<String>,
    key: Option<String>,
    known_hosts_strategy: KnownHosts,
}

#[async_trait::async_trait]
impl bb8::ManageConnection for Manager {
    type Connection = Sftp;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut session = SessionBuilder::default();

        if let Some(user) = &self.user {
            session.user(user.clone());
        }

        if let Some(key) = &self.key {
            session.keyfile(key);
        }

        session.known_hosts_check(self.known_hosts_strategy.clone());

        let session = session
            .connect(&self.endpoint)
            .await
            .map_err(parse_ssh_error)?;

        let sftp = Sftp::from_session(session, SftpOptions::default())
            .await
            .map_err(parse_sftp_error)?;

        if !self.root.is_empty() {
            let mut fs = sftp.fs();

            let paths = Path::new(&self.root).components();
            let mut current = PathBuf::new();
            for p in paths {
                current.push(p);
                let res = fs.create_dir(p).await;

                if let Err(e) = res {
                    // ignore error if dir already exists
                    if !is_sftp_protocol_error(&e) {
                        return Err(parse_sftp_error(e));
                    }
                }
                fs.set_cwd(&current);
            }
        }

        debug!("sftp connection created at {}", self.root);
        Ok(sftp)
    }

    // Check if connect valid by checking the root path.
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let _ = conn.fs().metadata("./").await.map_err(parse_sftp_error)?;

        Ok(())
    }

    /// Always allow reuse conn.
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
