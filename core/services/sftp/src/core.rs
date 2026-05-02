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
    pub acquire_timeout: Duration,
    pub connect_timeout: Duration,
    client: Arc<bounded::Pool<Manager>>,
}

pub struct SftpConnectionOptions {
    pub user: Option<String>,
    pub key: Option<String>,
    pub known_hosts_strategy: KnownHosts,
    pub acquire_timeout: Duration,
    pub connect_timeout: Duration,
}

impl Debug for SftpCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpCore")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

impl SftpCore {
    pub fn new(
        info: Arc<AccessorInfo>,
        endpoint: String,
        root: String,
        options: SftpConnectionOptions,
    ) -> Self {
        let client = bounded::Pool::new(
            bounded::PoolConfig::new(64),
            Manager {
                endpoint: endpoint.clone(),
                root: root.clone(),
                user: options.user,
                key: options.key,
                known_hosts_strategy: options.known_hosts_strategy.clone(),
                connect_timeout: options.connect_timeout,
            },
        );

        SftpCore {
            info,
            endpoint,
            root,
            acquire_timeout: options.acquire_timeout,
            connect_timeout: options.connect_timeout,
            client,
        }
    }

    pub async fn connect(&self) -> Result<bounded::Object<Manager>> {
        acquire_pooled_sftp_connection(&self.client, self.acquire_timeout).await
    }
}

// Only apply acquire timeout when the pool is saturated. Otherwise `fastpool::get()`
// may perform connection creation, and wrapping that path in a generic timeout would
// hide the underlying SSH/SFTP error again.
async fn acquire_pooled_sftp_connection<M>(
    pool: &Arc<bounded::Pool<M>>,
    acquire_timeout: Duration,
) -> Result<bounded::Object<M>>
where
    M: ManageObject<Error = Error>,
{
    let status = pool.status();
    let should_timeout = !acquire_timeout.is_zero()
        && status.current_size >= status.max_size
        && status.idle_count == 0;

    if should_timeout {
        return match tokio::time::timeout(acquire_timeout, pool.get()).await {
            Ok(result) => result,
            Err(_) => Err(Error::new(
                ErrorKind::Unexpected,
                "timed out waiting for pooled sftp connection",
            )
            .set_temporary()),
        };
    }

    pool.get().await
}

pub struct Manager {
    endpoint: String,
    root: String,
    user: Option<String>,
    key: Option<String>,
    known_hosts_strategy: KnownHosts,
    connect_timeout: Duration,
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

        session.connect_timeout(self.connect_timeout);
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Clone, Default)]
    struct TestManager {
        create_delay: Duration,
        recycle_delay: Duration,
        created: Arc<AtomicUsize>,
    }

    impl ManageObject for TestManager {
        type Object = usize;
        type Error = Error;

        async fn create(&self) -> Result<Self::Object> {
            if !self.create_delay.is_zero() {
                tokio::time::sleep(self.create_delay).await;
            }

            Ok(self.created.fetch_add(1, Ordering::SeqCst))
        }

        async fn is_recyclable(
            &self,
            _: &mut Self::Object,
            _: &ObjectStatus,
        ) -> Result<(), Self::Error> {
            if !self.recycle_delay.is_zero() {
                tokio::time::sleep(self.recycle_delay).await;
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn acquire_timeout_only_applies_when_pool_is_saturated() {
        let pool = bounded::Pool::new(
            bounded::PoolConfig::new(1),
            TestManager {
                create_delay: Duration::from_millis(50),
                ..Default::default()
            },
        );

        let started = std::time::Instant::now();
        let conn = acquire_pooled_sftp_connection(&pool, Duration::from_millis(10))
            .await
            .expect("pool should create a new connection");

        assert!(started.elapsed() >= Duration::from_millis(50));
        drop(conn);
    }

    #[tokio::test]
    async fn acquire_timeout_reports_waiting_for_pooled_connection() {
        let pool = bounded::Pool::new(bounded::PoolConfig::new(1), TestManager::default());
        let held = pool.get().await.expect("first connection should succeed");

        let err = acquire_pooled_sftp_connection(&pool, Duration::from_millis(20))
            .await
            .expect_err("second acquire should time out");

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());
        assert!(
            err.to_string()
                .contains("timed out waiting for pooled sftp connection")
        );

        drop(held);
    }
}
