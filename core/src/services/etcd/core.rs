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

use bb8::Pool;
use bb8::PooledConnection;
use bb8::RunError;
use etcd_client::Client;
use etcd_client::ConnectOptions;
use mea::once::OnceCell;

use crate::services::etcd::error::format_etcd_error;
use crate::{Buffer, Error, ErrorKind, Result};

pub mod constants {
    pub const DEFAULT_ETCD_ENDPOINTS: &str = "http://127.0.0.1:2379";
}

#[derive(Clone)]
pub struct Manager {
    endpoints: Vec<String>,
    options: ConnectOptions,
}

impl bb8::ManageConnection for Manager {
    type Connection = Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = Client::connect(self.endpoints.clone(), Some(self.options.clone()))
            .await
            .map_err(format_etcd_error)?;

        Ok(conn)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let _ = conn.status().await.map_err(format_etcd_error)?;
        Ok(())
    }

    /// Always allow reuse conn.
    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct EtcdCore {
    pub endpoints: Vec<String>,
    pub options: ConnectOptions,
    pub client: OnceCell<Pool<Manager>>,
}

impl Debug for EtcdCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdCore")
            .field("endpoints", &self.endpoints.join(","))
            .field("options", &self.options.clone())
            .finish_non_exhaustive()
    }
}

impl EtcdCore {
    pub async fn conn(&self) -> Result<PooledConnection<'static, Manager>> {
        let client = self
            .client
            .get_or_try_init(|| async {
                Pool::builder()
                    .max_size(64)
                    .build(Manager {
                        endpoints: self.endpoints.clone(),
                        options: self.options.clone(),
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

    pub async fn has_prefix(&self, prefix: &str) -> Result<bool> {
        let mut client = self.conn().await?;
        let get_options = Some(
            etcd_client::GetOptions::new()
                .with_prefix()
                .with_keys_only()
                .with_limit(1),
        );
        let resp = client
            .get(prefix, get_options)
            .await
            .map_err(format_etcd_error)?;
        Ok(!resp.kvs().is_empty())
    }
}

impl EtcdCore {
    pub async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let mut client = self.conn().await?;
        let resp = client.get(key, None).await.map_err(format_etcd_error)?;
        if let Some(kv) = resp.kvs().first() {
            Ok(Some(Buffer::from(kv.value().to_vec())))
        } else {
            Ok(None)
        }
    }

    pub async fn set(&self, key: &str, value: Buffer) -> Result<()> {
        let mut client = self.conn().await?;
        let _ = client
            .put(key, value.to_vec(), None)
            .await
            .map_err(format_etcd_error)?;
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.conn().await?;
        let _ = client.delete(key, None).await.map_err(format_etcd_error)?;
        Ok(())
    }
}
