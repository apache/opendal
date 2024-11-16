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

#[cfg(feature = "tests")]
use std::time::Duration;
use std::vec;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::engine::Engine as _;
use bb8::{PooledConnection, RunError};
use rust_nebula::{
    graph::GraphQuery, HostAddress, SingleConnSessionConf, SingleConnSessionManager,
};
use snowflaked::sync::Generator;
use tokio::sync::OnceCell;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::services::NebulaGraphConfig;
use crate::*;

static GENERATOR: Generator = Generator::new(0);

impl Configurator for NebulaGraphConfig {
    type Builder = NebulaGraphBuilder;
    fn into_builder(self) -> Self::Builder {
        NebulaGraphBuilder { config: self }
    }
}

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct NebulaGraphBuilder {
    config: NebulaGraphConfig,
}

impl Debug for NebulaGraphBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("MysqlBuilder");

        d.field("config", &self.config).finish()
    }
}

impl NebulaGraphBuilder {
    /// Set the host addr of nebulagraph's graphd server
    pub fn host(&mut self, host: &str) -> &mut Self {
        if !host.is_empty() {
            self.config.host = Some(host.to_string());
        }
        self
    }

    /// Set the host port of nebulagraph's graphd server
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.config.port = Some(port);
        self
    }

    /// Set the username of nebulagraph's graphd server
    pub fn username(&mut self, username: &str) -> &mut Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_string());
        }
        self
    }

    /// Set the password of nebulagraph's graphd server
    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_string());
        }
        self
    }

    /// Set the space name of nebulagraph's graphd server
    pub fn space(&mut self, space: &str) -> &mut Self {
        if !space.is_empty() {
            self.config.space = Some(space.to_string());
        }
        self
    }

    /// Set the tag name of nebulagraph's graphd server
    pub fn tag(&mut self, tag: &str) -> &mut Self {
        if !tag.is_empty() {
            self.config.tag = Some(tag.to_string());
        }
        self
    }

    /// Set the key field name of the NebulaGraph service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the NebulaGraph service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.config.root = Some(root.to_string());
        }
        self
    }
}

impl Builder for NebulaGraphBuilder {
    const SCHEME: Scheme = Scheme::NebulaGraph;
    type Config = NebulaGraphConfig;

    fn build(self) -> Result<impl Access> {
        let host = match self.config.host.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "host is empty")
                    .with_context("service", Scheme::NebulaGraph))
            }
        };
        let port = match self.config.port {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "port is empty")
                    .with_context("service", Scheme::NebulaGraph))
            }
        };
        let username = match self.config.username.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "username is empty")
                    .with_context("service", Scheme::NebulaGraph))
            }
        };
        let password = match self.config.password.clone() {
            Some(v) => v,
            None => "".to_string(),
        };
        let space = match self.config.space.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "space is empty")
                    .with_context("service", Scheme::NebulaGraph))
            }
        };
        let tag = match self.config.tag.clone() {
            Some(v) => v,
            None => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "tag is empty")
                    .with_context("service", Scheme::NebulaGraph))
            }
        };
        let key_field = match self.config.key_field.clone() {
            Some(v) => v,
            None => "key".to_string(),
        };
        let value_field = match self.config.value_field.clone() {
            Some(v) => v,
            None => "value".to_string(),
        };
        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let mut session_config = SingleConnSessionConf::new(
            vec![HostAddress::new(&host, port)],
            username,
            password,
            Some(space),
        );
        // NebulaGraph use fbthrift for communication. fbthrift's max_buffer_size is default 4 KB,
        // which is too small to store something.
        // So we could set max_buffer_size to 10 MB so that NebulaGraph can store files with filesize < 1 MB at least.
        session_config.set_buf_size(1024 * 1024);
        session_config.set_max_buf_size(64 * 1024 * 1024);
        session_config.set_max_parse_response_bytes_count(254);

        Ok(NebulaGraphBackend::new(Adapter {
            session_pool: OnceCell::new(),
            session_config,

            tag,
            key_field,
            value_field,
        })
        .with_root(root.as_str()))
    }
}

/// Backend for NebulaGraph service
pub type NebulaGraphBackend = kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    session_pool: OnceCell<bb8::Pool<SingleConnSessionManager>>,
    session_config: SingleConnSessionConf,

    tag: String,
    key_field: String,
    value_field: String,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("session_config", &self.session_config)
            .field("tag", &self.tag)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish()
    }
}

impl Adapter {
    async fn get_session(&self) -> Result<PooledConnection<SingleConnSessionManager>> {
        let session_pool = self
            .session_pool
            .get_or_try_init(|| async {
                bb8::Pool::builder()
                    .max_size(64)
                    .build(SingleConnSessionManager::new(self.session_config.clone()))
                    .await
            })
            .await
            .map_err(|err| Error::new(ErrorKind::Unexpected, format!("{}", err)).set_temporary())?;

        session_pool.get().await.map_err(|err| match err {
            RunError::User(err) => {
                Error::new(ErrorKind::Unexpected, format!("{}", err)).set_temporary()
            }
            RunError::TimedOut => {
                Error::new(ErrorKind::Unexpected, "connection request: timeout").set_temporary()
            }
        })
    }
}

impl kv::Adapter for Adapter {
    type Scanner = kv::ScanStdIter<vec::IntoIter<Result<String>>>;

    fn info(&self) -> kv::Info {
        kv::Info::new(
            Scheme::NebulaGraph,
            &self.session_config.space.clone().unwrap(),
            Capability {
                read: true,
                write: true,
                write_total_max_size: Some(1024 * 1024),
                write_can_empty: true,
                delete: true,
                list: true,
                shared: true,
                ..Default::default()
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Buffer>> {
        let path = path.replace("'", "\\'").replace('"', "\\\"");
        let query = format!(
            "LOOKUP ON {} WHERE {}.{} == '{}' YIELD properties(vertex).{} AS {};",
            self.tag, self.tag, self.key_field, path, self.value_field, self.value_field
        );
        let mut sess = self.get_session().await?;
        let result = sess
            .query(&query)
            .await
            .map_err(parse_nebulagraph_session_error)?;
        if result.is_empty() {
            Ok(None)
        } else {
            let row = result
                .get_row_values_by_index(0)
                .map_err(parse_nebulagraph_dataset_error)?;
            let value = row
                .get_value_by_col_name(&self.value_field)
                .map_err(parse_nebulagraph_dataset_error)?;
            let base64_str = value.as_string().map_err(parse_nebulagraph_dataset_error)?;
            let value_str = BASE64.decode(base64_str).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "unhandled error from nebulagraph")
                    .set_source(err)
            })?;
            let buf = Buffer::from(value_str);
            Ok(Some(buf))
        }
    }

    async fn set(&self, path: &str, value: Buffer) -> Result<()> {
        #[cfg(feature = "tests")]
        let path_copy = path;

        self.delete(path).await?;
        let path = path.replace("'", "\\'").replace('"', "\\\"");
        let file = value.to_vec();
        let file = BASE64.encode(&file);
        let snowflake_id: u64 = GENERATOR.generate();
        let query = format!(
            "INSERT VERTEX {} VALUES {}:('{}', '{}');",
            self.tag, snowflake_id, path, file
        );
        let mut sess = self.get_session().await?;
        sess.execute(&query)
            .await
            .map_err(parse_nebulagraph_session_error)?;

        // To pass tests, we should confirm NebulaGraph has inserted data successfully
        #[cfg(feature = "tests")]
        loop {
            let v = self.get(path_copy).await.unwrap();
            if v.is_none() {
                std::thread::sleep(Duration::from_millis(1000));
            } else {
                break;
            }
        }
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let path = path.replace("'", "\\'").replace('"', "\\\"");
        let query = format!(
            "LOOKUP ON {} WHERE {}.{} == '{}' YIELD id(vertex) AS id | DELETE VERTEX $-.id;",
            self.tag, self.tag, self.key_field, path
        );
        let mut sess = self.get_session().await?;
        sess.execute(&query)
            .await
            .map_err(parse_nebulagraph_session_error)?;
        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Self::Scanner> {
        let path = path.replace("'", "\\'").replace('"', "\\\"");
        let query = format!(
            "LOOKUP ON {} WHERE {}.{} STARTS WITH '{}' YIELD properties(vertex).{} AS {};",
            self.tag, self.tag, self.key_field, path, self.key_field, self.key_field
        );

        let mut sess = self.get_session().await?;
        let result = sess
            .query(&query)
            .await
            .map_err(parse_nebulagraph_session_error)?;
        let mut res_vec = vec![];
        for row_i in 0..result.get_row_size() {
            let row = result
                .get_row_values_by_index(row_i)
                .map_err(parse_nebulagraph_dataset_error)?;
            let value = row
                .get_value_by_col_name(&self.key_field)
                .map_err(parse_nebulagraph_dataset_error)?;
            let sub_path = value.as_string().map_err(parse_nebulagraph_dataset_error)?;

            res_vec.push(Ok(sub_path));
        }
        Ok(kv::ScanStdIter::new(res_vec.into_iter()))
    }
}

fn parse_nebulagraph_session_error(err: rust_nebula::SingleConnSessionError) -> Error {
    Error::new(ErrorKind::Unexpected, "unhandled error from nebulagraph").set_source(err)
}

fn parse_nebulagraph_dataset_error(err: rust_nebula::DataSetError) -> Error {
    Error::new(ErrorKind::Unexpected, "unhandled error from nebulagraph").set_source(err)
}
