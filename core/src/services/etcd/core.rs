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

use etcd_client::Client;
use etcd_client::ConnectOptions;
use fastpool::ManageObject;
use fastpool::ObjectStatus;
use fastpool::bounded;
use std::fmt::Debug;
use std::sync::Arc;

use crate::raw::*;
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

impl ManageObject for Manager {
    type Object = Client;
    type Error = Error;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        Client::connect(self.endpoints.clone(), Some(self.options.clone()))
            .await
            .map_err(format_etcd_error)
    }

    async fn is_recyclable(
        &self,
        o: &mut Self::Object,
        _: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        match o.status().await {
            Ok(_) => Ok(()),
            Err(err) => Err(format_etcd_error(err)),
        }
    }
}

#[derive(Clone)]
pub struct EtcdCore {
    endpoints: Vec<String>,
    options: ConnectOptions,
    client: Arc<bounded::Pool<Manager>>,
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
    pub fn new(endpoints: Vec<String>, options: ConnectOptions) -> Self {
        let client = bounded::Pool::new(
            bounded::PoolConfig::new(64),
            Manager {
                endpoints: endpoints.clone(),
                options: options.clone(),
            },
        );

        Self {
            endpoints,
            options,
            client,
        }
    }

    pub async fn conn(&self) -> Result<bounded::Object<Manager>> {
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
