use crate::error::{other, BackendError};
use crate::ops::{OpCreate, OpDelete, OpList, OpRead, OpStat, OpWrite, Operation};
use crate::path::build_abs_path;
use crate::services::redis::dir_stream::DirStream;
use crate::services::redis::error::{
    new_deserialize_metadata_error, new_exec_async_cmd_error, new_get_async_con_error,
    new_serialize_metadata_error,
};
use crate::services::redis::REDIS_API_VERSION;
use crate::{Accessor, BytesReader, DirStreamer, ObjectMetadata, ObjectMode};
use anyhow::anyhow;
use async_compat::Compat;
use async_trait::async_trait;
use bytes::Bytes;
use futures::io::AsyncReadExt;
use futures::io::BufReader;
use num_traits::abs;
use redis::aio::Connection;
use redis::{AsyncCommands, Client, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use serde::Serialize;
use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::intrinsics::unreachable;
use std::io::{Cursor, Result};
use std::path::PathBuf;
use std::sync::Arc;
use time::OffsetDateTime;
use url::Url;

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

        let ep_url = Url::parse(&endpoint).map_err(|e| {
            other(BackendError::new(
                HashMap::from([("endpoint".to_string(), endpoint.clone())]),
                anyhow!("endpoint is invalid: {:?}", e),
            ))
        })?;

        let con_addr = match ep_url.scheme() {
            "tcp" | "redis" => {
                let host = ep_url
                    .host()
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let port = ep_url.port().unwrap_or(6379);
                ConnectionAddr::Tcp(host, port)
            }
            "tcps" | "rediss" => {
                let host = ep_url
                    .host()
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let port = ep_url.port().unwrap_or(6379);
                ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: false,
                }
            }
            "unix" | "redis+unix" => {
                let path = ep_url.to_file_path().map_err(|e| {
                    other(BackendError::new(
                        HashMap::from([("endpoint".to_string(), endpoint.to_string())]),
                        anyhow!("invalid path to unix socket: {:?}", e),
                    ))
                })?;
                ConnectionAddr::Unix(path)
            }
            s => {
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
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = build_abs_content_path(self.root.as_str(), args.path());
        let from = args.offset().map_or_else(|| 0, |offset| offset as isize);
        let to = args.size().map_or_else(|| -1, |size| size as isize + from);
        let mut con = self
            .client
            .get_async_connection()
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

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = args.path().to_string();

        let mut con = self
            .client
            .get_async_connection()
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

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = args.path().to_string();
        let mut con = self
            .client
            .get_async_connection()
            .await
            .map_err(|e| new_get_async_con_error(e, Operation::Stat, path.as_str()))?;

        let m_path = build_abs_meta_path(self.root.as_str(), path.as_str());
        let bin: Vec<u8> = con
            .get(m_path)
            .await
            .map_err(|e| new_exec_async_cmd_error(e, Operation::Stat, path.as_str()))?;

        bincode::deserialize(&bin[..]).map_err(new_deserialize_metadata_error(
            e,
            Operation::Stat,
            path.as_str(),
        ))
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = args.path().to_string();
        let mut con = self
            .client
            .get_async_connection()
            .await
            .map_err(|e| new_get_async_con_error(e, Operation::Create, path.as_str()))?;

        let mut meta = ObjectMetadata::default();
        meta.set_content_length(0);
        meta.set_mode(args.mode());
        if args.mode() == ObjectMode::FILE {
            // only set last_modified for files
            let last_modified = OffsetDateTime::now_utc();
            meta.set_last_modified(last_modified);
        }

        let m_path = build_abs_meta_path(self.root.as_str(), path.as_str());
        let ser = bincode::serialize(&meta)
            .map_err(|err| new_serialize_metadata_error(err, Operation::Create, path.as_str()))?;

        // update parent folders
        let mut entry = PathBuf::from(build_abs_path(self.root.as_str(), path.as_str()));
        while let Some(parent) = entry.parent() {
            let (p, c): (String, String) =
                (parent.display().to_string(), entry.display().to_string());

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
            .map_err(|err| new_exec_async_cmd_error(err, Operation::Write, entry.as_str()))
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = build_abs_path(self.root.as_str(), args.path());
        let mut con = self
            .client
            .get_async_connection()
            .await
            .map_err(|err| new_exec_async_cmd_error(err, Operation::List, path.as_str()))?;
        let mut it = con
            .sscan::<&str, String>(path.as_str())
            .await
            .map_err(|err| new_exec_async_cmd_error(err, Operation::List, path.as_str()))?;
        Ok(Box::new(DirStream::new(
            Arc::new(self.clone()),
            Arc::new(con),
            path.as_str(),
            it,
        )))
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = args.path();
        let abs_path = build_abs_path(self.root.as_str(), path);

        let mut con = self
            .client
            .get_async_connection()
            .await
            .map_err(|err| new_get_async_con_error(err, Operation::Delete, &abs_path))?;

        // a reversed-level-order traversal delete
        // recursion is not friendly for rust, and on-heap stack is preferred.
        let (mut queue, mut rev_stack) = (VecDeque::new(), VecDeque::new());

        let mut current = path.to_string();
        queue.push_back(current);
        while let Some(pop) = queue.pop_front() {
            let skey = format!("v{}:k:{}", REDIS_API_VERSION, pop);
            let mut it = con
                .sscan(skey)
                .await
                .map_err(|err| new_exec_async_cmd_error(err, Operation::Delete, &abs_path))?;
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
            con.srem(skey, abs_path).await.map_err(|err| {
                new_exec_async_cmd_error(err, Operation::Delete, abs_path.as_str())
            })?;
        }

        Ok(())
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
async fn remove(con: &mut Connection, path: &str, to_remove: &str) -> Result<()> {
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
