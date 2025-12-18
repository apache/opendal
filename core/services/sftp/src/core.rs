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

use super::error::is_sftp_protocol_error;
use super::error::parse_sftp_error;
use super::error::parse_ssh_error;
use fastpool::{ManageObject, ObjectStatus, bounded};
use log::debug;
use opendal_core::raw::*;
use opendal_core::*;
use openssh::KnownHosts;
use openssh::SessionBuilder;
use openssh_sftp_client::Sftp;
use openssh_sftp_client::SftpOptions;
use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub struct SftpCore {
    pub info: Arc<AccessorInfo>,
    pub endpoint: String,
    pub root: String,
    client: Arc<bounded::Pool<Manager>>,
}

impl Debug for SftpCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpCore")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl SftpCore {
    pub fn new(
        info: Arc<AccessorInfo>,
        endpoint: String,
        root: String,
        user: Option<String>,
        key: Option<String>,
        known_hosts_strategy: KnownHosts,
    ) -> Self {
        let client = bounded::Pool::new(
            bounded::PoolConfig::new(64),
            Manager {
                endpoint: endpoint.clone(),
                root: root.clone(),
                user,
                key,
                known_hosts_strategy,
            },
        );

        SftpCore {
            info,
            endpoint,
            root,
            client,
        }
    }

    pub async fn connect(&self) -> Result<bounded::Object<Manager>> {
        let fut = self.client.get();

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                Err(Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary())
            }
            result = fut => match result {
                Ok(conn) => Ok(conn),
                Err(err) => Err(err),
            }
        }
    }
}

pub struct Manager {
    endpoint: String,
    root: String,
    user: Option<String>,
    key: Option<String>,
    known_hosts_strategy: KnownHosts,
}

impl ManageObject for Manager {
    type Object = Sftp;
    type Error = Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
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
    async fn is_recyclable(
        &self,
        o: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        match o.fs().metadata("./").await {
            Ok(_) => Ok(()),
            Err(e) => Err(parse_sftp_error(e)),
        }
    }
}
