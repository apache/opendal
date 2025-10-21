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

use tikv_client::Config;
use tikv_client::RawClient;
use tokio::sync::OnceCell;

use crate::*;

/// TikvCore holds the configuration and client for interacting with TiKV.
#[derive(Clone)]
pub struct TikvCore {
    pub client: OnceCell<RawClient>,
    pub endpoints: Vec<String>,
    pub insecure: bool,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Debug for TikvCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TikvCore")
            .field("endpoints", &self.endpoints)
            .field("insecure", &self.insecure)
            .finish()
    }
}

impl TikvCore {
    async fn get_connection(&self) -> Result<RawClient> {
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }
        let client = if self.insecure {
            RawClient::new(self.endpoints.clone())
                .await
                .map_err(parse_tikv_config_error)?
        } else if self.ca_path.is_some() && self.key_path.is_some() && self.cert_path.is_some() {
            let (ca_path, key_path, cert_path) = (
                self.ca_path.clone().unwrap(),
                self.key_path.clone().unwrap(),
                self.cert_path.clone().unwrap(),
            );
            let config = Config::default().with_security(ca_path, cert_path, key_path);
            RawClient::new_with_config(self.endpoints.clone(), config)
                .await
                .map_err(parse_tikv_config_error)?
        } else {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "invalid configuration")
                    .with_context("service", Scheme::Tikv)
                    .with_context("endpoints", format!("{:?}", self.endpoints)),
            );
        };
        self.client.set(client.clone()).ok();
        Ok(client)
    }

    pub async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let result = self
            .get_connection()
            .await?
            .get(path.to_owned())
            .await
            .map_err(parse_tikv_error)?;
        Ok(result.map(Buffer::from))
    }

    pub async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        self.get_connection()
            .await?
            .put(path.to_owned(), value.to_vec())
            .await
            .map_err(parse_tikv_error)
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        self.get_connection()
            .await?
            .delete(path.to_owned())
            .await
            .map_err(parse_tikv_error)
    }
}

fn parse_tikv_error(e: tikv_client::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "error from tikv").set_source(e)
}

fn parse_tikv_config_error(e: tikv_client::Error) -> Error {
    Error::new(ErrorKind::ConfigInvalid, "invalid configuration")
        .with_context("service", Scheme::Tikv)
        .set_source(e)
}
