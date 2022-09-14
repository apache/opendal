// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Cursor;
use std::io::Result;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_compat::Compat;
use async_trait::async_trait;
use futures::io::AsyncReadExt;
use http::Uri;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use redis::Client;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::RedisConnectionInfo;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::error::new_deserialize_metadata_error;
use super::error::new_exec_async_cmd_error;
use super::error::new_get_async_con_error;
use super::error::new_serialize_metadata_error;
use super::REDIS_API_VERSION;
use crate::error::other;
use crate::error::BackendError;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::path::build_abs_path;
use crate::Accessor;
use crate::BytesReader;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;

const DEFAULT_REDIS_ENDPOINT: &str = "tcp://127.0.0.1:6543";

#[derive(Clone, Default)]
pub struct Builder {
    endpoint: Option<String>,
    username: Option<String>,
    password: Option<String>,
    root: Option<String>,
    // the number of DBs redis can take is unlimited
    // DB 0 will be used by default
    db: i64,
}

impl Builder {
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoint = Some(endpoint.to_owned());
        }
        self
    }

    pub fn username(&mut self, username: &str) -> &mut Self {
        if !username.is_empty() {
            self.username = Some(username.to_owned());
        }
        self
    }

    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.password = Some(password.to_owned());
        }
        self
    }
    pub fn db(&mut self, db: i64) -> &mut Self {
        self.db = db;
        self
    }
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");
        ds.field("db", &self.db.to_string());
        if let Some(root) = self.root.clone() {
            ds.field("root", &root);
        }
        if let Some(endpoint) = self.endpoint.clone() {
            ds.field("endpoint", &endpoint);
        }
        if let Some(username) = self.username.clone() {
            ds.field("username", &username);
        }
        if self.password.is_some() {
            ds.field("password", &"<redacted>");
        }
        ds.finish()
    }
}

impl Builder {
    pub fn build(&mut self) -> Result<Backend> {
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| DEFAULT_REDIS_ENDPOINT.to_string());

        let ep_url = endpoint.parse::<Uri>().map_err(|e| {
            other(BackendError::new(
                HashMap::from([("endpoint".to_string(), endpoint.clone())]),
                anyhow!("endpoint is invalid: {:?}", e),
            ))
        })?;

        let con_addr = match ep_url.scheme_str() {
            Some("tcp") | Some("redis") | None => {
                let host = ep_url
                    .host()
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let port = ep_url.port_u16().unwrap_or(6379);
                ConnectionAddr::Tcp(host, port)
            }
            Some("tcps") | Some("rediss") => {
                let host = ep_url
                    .host()
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let port = ep_url.port_u16().unwrap_or(6379);
                ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: false,
                }
            }
            Some("unix") | Some("redis+unix") => {
                let path = PathBuf::from(ep_url.path());
                ConnectionAddr::Unix(path)
            }
            Some(s) => {
                return Err(other(BackendError::new(
                    HashMap::from([("endpoint".to_string(), endpoint)]),
                    anyhow!("invalid or unsupported URL scheme: {}", s),
                )))
            }
        };

        let redis_info = RedisConnectionInfo {
            db: self.db,
            username: self.username.clone(),
            password: self.password.clone(),
        };

        let con_info = ConnectionInfo {
            addr: con_addr,
            redis: redis_info,
        };

        let client = Client::open(con_info)
            .map_err(|e| other(anyhow!("establish redis client error: {:?}", e)))?;

        let root = self.root.clone().unwrap_or_else(|| "/".to_string());
        Ok(Backend { client, root })
    }
}

#[derive(Clone)]
pub struct Backend {
    client: Client,
    root: String,
}

impl Backend {
    pub fn root(&self) -> String {
        self.root.clone()
    }
}

// implement `Debug` manually, or password may be leaked.
impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Backend");
        ds.field("root", &self.root);

        let info = self.client.get_connection_info();

        ds.field("addr", &info.addr);
        ds.field("db", &info.redis.db);

        if let Some(username) = info.redis.username.clone() {
            ds.field("user", &username);
        }
        if info.redis.password.is_some() {
            ds.field("password", &"<redacted>");
        }

        ds.finish()
    }
}

#[async_trait]
impl Accessor for Backend {
    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        let mut path = path.to_string();
        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|e| new_get_async_con_error(e, Operation::Create, path.as_str()))?;

        let mut meta = ObjectMetadata::default();
        meta.set_mode(args.mode());
        if args.mode() == ObjectMode::FILE {
            // only set last_modified for files
            let last_modified = OffsetDateTime::now_utc();
            meta.set_last_modified(last_modified);
        } else if args.mode() == ObjectMode::DIR && !path.ends_with('/') {
            // folder's name must be ends with '/'!
            path += "/";
        }

        let m_path = build_abs_meta_path(self.root.as_str(), path.as_str());
        let ser = bincode::serialize(&meta)
            .map_err(|err| new_serialize_metadata_error(err, Operation::Create, path.as_str()))?;

        // update parent folders
        let entry = PathBuf::from(build_abs_path(self.root.as_str(), path.as_str()));
        while let Some(parent) = entry.parent() {
            let (p, c): (String, String) = (
                // directory's name must be end with '/'!
                parent.display().to_string() + "/",
                entry.display().to_string(),
            );

            // update parent folders' children
            let to_children = format!("v{}:ch:{}", REDIS_API_VERSION, p);
            con.sadd(to_children, c)
                .await
                .map_err(|err| new_exec_async_cmd_error(err, Operation::Create, path.as_str()))?;

            // update parent folders' metadata
            let mut metadata = ObjectMetadata::default();
            metadata.set_mode(ObjectMode::DIR);
            let parent_meta = format!("v{}:m:{}", REDIS_API_VERSION, p);
            let bin = bincode::serialize(&metadata).map_err(|err| {
                new_serialize_metadata_error(err, Operation::Create, path.as_str())
            })?;
            con.set(parent_meta, bin)
                .await
                .map_err(|err| new_exec_async_cmd_error(err, Operation::Create, path.as_str()))?;
        }

        con.set(m_path, ser)
            .await
            .map_err(|err| new_exec_async_cmd_error(err, Operation::Write, path.as_str()))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let path = build_abs_content_path(self.root.as_str(), path);
        let from = args.offset().map_or_else(|| 0, |offset| offset as isize);
        let to = args.size().map_or_else(|| -1, |size| size as isize + from);
        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|e| new_get_async_con_error(e, Operation::Read, path.as_str()))?;
        let gr = con
            .getrange::<&str, Vec<u8>>(path.as_str(), from, to)
            .await
            .map_err(|e| new_exec_async_cmd_error(e, Operation::Read, path.as_str()))?;
        let cur = Cursor::new(gr);
        let compat = Compat::new(cur);
        Ok(Box::new(compat) as BytesReader)
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let path = path.to_string();

        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|e| new_get_async_con_error(e, Operation::Write, path.as_str()))?;

        let c_path = build_abs_content_path(self.root.as_str(), path.as_str());
        let size = args.size();
        let mut reader = r;
        // TODO: asynchronous writing
        let mut buf = vec![];
        let content_length = reader.read_to_end(&mut buf).await?;
        con.set(c_path, buf)
            .await
            .map_err(|e| new_exec_async_cmd_error(e, Operation::Write, &path))?;

        let m_path = build_abs_meta_path(self.root.as_str(), path.as_str());
        let mut meta = ObjectMetadata::default();
        let mode = if path.ends_with('/') {
            ObjectMode::DIR
        } else {
            ObjectMode::FILE
        };
        let last_modified = OffsetDateTime::now_utc();
        meta.set_content_length(content_length as u64);
        meta.set_mode(mode);
        meta.set_last_modified(last_modified);
        let ser = bincode::serialize(&meta)
            .map_err(|e| new_serialize_metadata_error(e, Operation::Write, path.as_str()))?;
        con.set(m_path, ser)
            .await
            .map_err(|e| new_exec_async_cmd_error(e, Operation::Write, path.as_str()))?;

        Ok(size)
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<ObjectMetadata> {
        let path = path.to_string();
        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|e| new_get_async_con_error(e, Operation::Stat, path.as_str()))?;

        let m_path = build_abs_meta_path(self.root.as_str(), path.as_str());
        let bin: Vec<u8> = con
            .get(m_path)
            .await
            .map_err(|e| new_exec_async_cmd_error(e, Operation::Stat, path.as_str()))?;

        bincode::deserialize(&bin[..])
            .map_err(|e| new_deserialize_metadata_error(e, Operation::Stat, path.as_str()))
    }

    async fn delete(&self, path: &str, _args: OpDelete) -> Result<()> {
        let path = path.to_string();
        let abs_path = build_abs_path(self.root.as_str(), path.as_str());

        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|err| new_get_async_con_error(err, Operation::Delete, path.as_str()))?;

        // a reversed-level-order traversal delete
        // recursion is not friendly for rust, and on-heap stack is preferred.
        let (mut queue, mut rev_stack) = (VecDeque::new(), VecDeque::new());

        queue.push_back(path.to_string());
        while let Some(pop) = queue.pop_front() {
            let skey = format!("v{}:k:{}", REDIS_API_VERSION, pop);
            let mut it = con
                .sscan(skey)
                .await
                .map_err(|err| new_exec_async_cmd_error(err, Operation::Delete, path.as_str()))?;
            while let Some(child) = it.next_item().await {
                let _ = queue.push_back(child);
            }
            let _ = rev_stack.push_back(pop);
        }

        while let Some(pop) = rev_stack.pop_back() {
            remove(&mut con, abs_path.as_str(), pop.as_str()).await?;
        }

        if let Some(parent) = PathBuf::from(abs_path.clone()).parent() {
            let p = parent.display().to_string();
            let skey = format!("v{}:k:{}", REDIS_API_VERSION, p);
            con.srem(skey, abs_path.clone()).await.map_err(|err| {
                new_exec_async_cmd_error(err, Operation::Delete, abs_path.as_str())
            })?;
        }

        Ok(())
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<DirStreamer> {
        let path = build_abs_path(self.root.as_str(), path);
        let con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|err| new_exec_async_cmd_error(err, Operation::List, path.as_str()))?;
        Ok(Box::new(DirStream::new(
            Arc::new(self.clone()),
            con,
            path.as_str(),
        )))
    }
}

fn build_abs_content_path<'a>(root: &'a str, rel_path: &'a str) -> String {
    format!(
        "v{}:c:{}",
        REDIS_API_VERSION,
        build_abs_path(root, rel_path)
    )
}

/// remove all data of the `to_remove` entry
async fn remove(con: &mut ConnectionManager, path: &str, to_remove: &str) -> Result<()> {
    let m_path = format!("v{}:m:{}", REDIS_API_VERSION, to_remove);
    let c_path = format!("v{}:c:{}", REDIS_API_VERSION, to_remove);
    let s_path = format!("v{}:k:{}", REDIS_API_VERSION, to_remove);
    con.del(&[m_path, c_path, s_path])
        .await
        .map_err(|err| new_exec_async_cmd_error(err, Operation::Delete, path))
}

fn build_abs_meta_path<'a>(root: &'a str, rel_path: &'a str) -> String {
    format!(
        "v{}:m:{}",
        REDIS_API_VERSION,
        build_abs_path(root, rel_path)
    )
}

fn build_abs_to_child_path(root: &str, rel_path: &str) -> String {
    format!(
        "v{}:ch:{}",
        REDIS_API_VERSION,
        build_abs_path(root, rel_path)
    )
}

#[cfg(test)]
mod bin_test {
    use crate::ObjectMetadata;

    #[test]
    fn test_generate_bincode() {
        let meta = ObjectMetadata::default();
        println!("{:?}", bincode::serialize(&meta).unwrap());
    }
}
