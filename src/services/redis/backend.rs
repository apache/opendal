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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Result;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::io::AsyncReadExt;
use futures::io::Cursor;
use http::Uri;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use redis::Client;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::RedisConnectionInfo;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::error::new_async_connection_error;
use super::error::new_deserialize_metadata_error;
use super::error::new_exec_async_cmd_error;
use super::error::new_serialize_metadata_error;
use super::v0_children_prefix;
use super::v0_content_prefix;
use super::v0_meta_prefix;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::path::build_abs_path;
use crate::path::normalize_root;
use crate::Accessor;
use crate::BytesReader;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;

const DEFAULT_REDIS_ENDPOINT: &str = "tcp://127.0.0.1:6379";
const DEFAULT_REDIS_PORT: u16 = 6379;

/// Redis backend builder
#[derive(Clone, Default)]
pub struct Builder {
    /// network address of the Redis service. Can be "tcp://127.0.0.1:6379", e.g.
    ///
    /// default is "tcp://127.0.0.1:6379"
    endpoint: Option<String>,
    /// the username to connect redis service.
    ///
    /// default is None
    username: Option<String>,
    /// the password for authentication
    ///
    /// default is None
    password: Option<String>,
    /// the working directory of the Redis service. Can be "/path/to/dir"
    ///
    /// default is "/"
    root: Option<String>,
    /// the number of DBs redis can take is unlimited
    ///
    /// default is db 0
    db: i64,
}

impl Builder {
    /// set the network address of redis service.
    ///
    /// currently supported schemes:
    /// - no scheme: will be seen as "tcp"
    /// - "tcp" or "redis": unsecured redis connections
    /// - "unix" or "redis+unix": unix socket connection
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoint = Some(endpoint.to_owned());
        }
        self
    }

    /// set the username for redis
    ///
    /// default: no username
    pub fn username(&mut self, username: &str) -> &mut Self {
        if !username.is_empty() {
            self.username = Some(username.to_owned());
        }
        self
    }

    /// set the password for redis
    ///
    /// default: no password
    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.password = Some(password.to_owned());
        }
        self
    }

    /// set the db used in redis
    ///
    /// default: 0
    pub fn db(&mut self, db: i64) -> &mut Self {
        self.db = db;
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }

    /// Establish connection to Redis and finish making Redis endpoint
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
                let port = ep_url.port_u16().unwrap_or(DEFAULT_REDIS_PORT);
                ConnectionAddr::Tcp(host, port)
            }
            // TODO: wait for upstream to support `rustls` based TLS connection.
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

        let client = Client::open(con_info).map_err(|e| {
            other(BackendError::new(
                HashMap::from([
                    ("endpoint".to_string(), endpoint),
                    ("db".to_string(), self.db.to_string()),
                    (
                        "username".to_string(),
                        self.username
                            .clone()
                            .unwrap_or_else(|| "<None>".to_string()),
                    ),
                ]),
                anyhow!("establish redis client error: {:?}", e),
            ))
        })?;

        let root = normalize_root(
            self.root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );
        Ok(Backend { client, root })
    }
}

impl Debug for Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");
        ds.field("db", &self.db.to_string());
        ds.field("root", &self.root);
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

/// Redis Backend of opendal
#[derive(Clone)]
pub struct Backend {
    client: Client,
    root: String,
}

impl Backend {
    /// `remove` takes only absolute paths, and brutely delete all data held in `abs_path`. Its parent folders will not be affected
    ///
    /// if the directory to remove is not empty, an `other` error will be emitted.
    async fn remove(con: &mut ConnectionManager, abs_path: &str) -> Result<()> {
        let m_path = v0_meta_prefix(abs_path);
        let c_path = v0_content_prefix(abs_path);
        let s_path = v0_children_prefix(abs_path);

        // it is ok to remove not existing keys.
        con.del(&[m_path, c_path, s_path])
            .await
            .map_err(|err| new_exec_async_cmd_error(err, Operation::Delete, abs_path))
    }

    /// `mkdir_current` will record the metadata of `to_create`
    ///
    /// Note: `to_create` have to be an absolute path.
    async fn mkdir_current(con: &mut ConnectionManager, to_create: &str, path: &str) -> Result<()> {
        let mut metadata = ObjectMetadata::default();
        metadata.set_mode(ObjectMode::DIR);

        let to_create = if !to_create.ends_with('/') {
            to_create.to_string() + "/"
        } else {
            to_create.to_string()
        };

        let path = path.to_string();
        let bin = bincode::serialize(&metadata)
            .map_err(|err| new_serialize_metadata_error(err, Operation::Create, path.as_str()))?;

        let m_path = v0_meta_prefix(to_create.as_str());
        con.set(m_path, bin)
            .await
            .map_err(|err| new_exec_async_cmd_error(err, Operation::Create, path.as_str()))?;

        Ok(())
    }

    /// `mkdir` works like `mkdir -p` in *NIX systems, this will create `to_create` and all of its missing parent directories
    async fn mkdir(con: &mut ConnectionManager, to_create: &str, path: &str) -> Result<()> {
        let path = path.to_string();
        Backend::mkdir_current(con, to_create, path.as_str()).await?;

        let mut current = to_create.to_string();
        while let Some(parent) = PathBuf::from(current).parent() {
            let p = parent.display().to_string() + "/";
            let this_generation = v0_children_prefix(p.as_str());
            con.sadd(this_generation, to_create)
                .await
                .map_err(|err| new_exec_async_cmd_error(err, Operation::Create, path.as_str()))?;

            Backend::mkdir_current(con, p.as_str(), path.as_str()).await?;
            current = p;
        }

        Ok(())
    }
}

impl Backend {
    /// build from iterator
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();
        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "endpoint" => builder.endpoint(v),
                "username" => builder.username(v),
                "password" => builder.password(v),
                "db" => match v.parse::<i64>() {
                    Ok(num) => builder.db(num),
                    _ => continue,
                },
                _ => continue,
            };
        }
        builder.build()
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

        // establish connection to redis
        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|e| new_async_connection_error(e, Operation::Create, path.as_str()))?;

        // create current dir
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

        let abs_path = build_abs_path(self.root.as_str(), path.as_str());

        let m_path = v0_meta_prefix(abs_path.as_str());
        let bin = bincode::serialize(&meta)
            .map_err(|err| new_serialize_metadata_error(err, Operation::Create, path.as_str()))?;

        // update parent folders
        if let Some(parent) = PathBuf::from(abs_path).parent() {
            let p = parent.display().to_string();
            Backend::mkdir(&mut con, p.as_str(), path.as_str()).await?;
        }

        con.set(m_path, bin)
            .await
            .map_err(|err| new_exec_async_cmd_error(err, Operation::Write, path.as_str()))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let c_path = v0_content_prefix(build_abs_path(self.root.as_str(), path).as_str());

        // &str.clone() is unrecommended by clippy
        let path = path.to_string();

        // get range parameters
        let from = args.offset().map_or_else(|| 0, |offset| offset as isize);
        let to = args.size().map_or_else(|| -1, |size| size as isize + from);

        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|e| new_async_connection_error(e, Operation::Read, path.as_str()))?;

        let gr = con
            .getrange::<&str, Vec<u8>>(c_path.as_str(), from, to)
            .await
            .map_err(|e| new_exec_async_cmd_error(e, Operation::Read, path.as_str()))?;

        let cur = Cursor::new(gr);
        Ok(Box::new(cur) as BytesReader)
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let abs_path = build_abs_path(self.root.as_str(), path);
        let path = path.to_string();

        if path.ends_with('/') {
            return Err(other(ObjectError::new(
                Operation::Write,
                path.as_str(),
                anyhow!("cannot write to directory"),
            )));
        }

        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|e| new_async_connection_error(e, Operation::Write, path.as_str()))?;

        let c_path = v0_content_prefix(abs_path.as_str());
        let size = args.size();
        let mut reader = r;

        // TODO: asynchronous writing with `SETRANGE`

        let mut buf = vec![];
        let content_length = reader.read_to_end(&mut buf).await?;
        con.set(c_path, buf)
            .await
            .map_err(|e| new_exec_async_cmd_error(e, Operation::Write, &path))?;

        let m_path = v0_meta_prefix(abs_path.as_str());

        let mut meta = ObjectMetadata::default();
        let mode = ObjectMode::FILE;
        let last_modified = OffsetDateTime::now_utc();
        meta.set_content_length(content_length as u64);
        meta.set_mode(mode);
        meta.set_last_modified(last_modified);

        // serialize and write to redis backend
        let bin = bincode::serialize(&meta)
            .map_err(|e| new_serialize_metadata_error(e, Operation::Write, path.as_str()))?;
        con.set(m_path, bin)
            .await
            .map_err(|e| new_exec_async_cmd_error(e, Operation::Write, path.as_str()))?;

        Ok(size)
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<ObjectMetadata> {
        let abs_path = build_abs_path(self.root.as_str(), path);
        let path = path.to_string();

        let mut con = self
            .client
            .get_tokio_connection_manager()
            .await
            .map_err(|e| new_async_connection_error(e, Operation::Stat, path.as_str()))?;

        let m_path = v0_meta_prefix(abs_path.as_str());
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
            .map_err(|err| new_async_connection_error(err, Operation::Delete, path.as_str()))?;

        let children = v0_children_prefix(abs_path.as_str());
        let mut child_iter = con
            .sscan::<String, String>(children)
            .await
            .map_err(|err| new_exec_async_cmd_error(err, Operation::Delete, path.as_str()))?;

        // directory is not empty
        if child_iter.next_item().await.is_some() {
            return Err(other(ObjectError::new(
                Operation::Delete,
                path.as_str(),
                anyhow!("Directory not empty!"),
            )));
        }

        Backend::remove(&mut con, abs_path.as_str()).await?;

        if let Some(parent) = PathBuf::from(abs_path.clone()).parent() {
            let this_generation = v0_children_prefix(parent.display().to_string().as_str()) + "/";

            con.srem(this_generation, &abs_path)
                .await
                .map_err(|err| new_exec_async_cmd_error(err, Operation::Delete, path.as_str()))?;
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

#[cfg(test)]
mod redis_test {
    use time::OffsetDateTime;

    use super::Builder;
    use crate::ObjectMetadata;
    use crate::ObjectMode;

    const TIMESTAMP: i64 = 7355608;
    const CONTENT_LENGTH: u64 = 123456;

    // this function is used for generating testing binary data
    #[test]
    fn test_generate_bincode() {
        let mut meta = ObjectMetadata::default();
        meta.set_mode(ObjectMode::DIR);
        println!(
            "serialized directory metadata: {:?}",
            bincode::serialize(&meta).unwrap()
        );

        meta.set_mode(ObjectMode::FILE);
        let datetime = OffsetDateTime::from_unix_timestamp(TIMESTAMP).unwrap();
        meta.set_last_modified(datetime);
        meta.set_content_length(CONTENT_LENGTH);

        println!(
            "serialized file metadata: {:?}",
            bincode::serialize(&meta).unwrap()
        );
    }

    #[test]
    fn test_deserialize() {
        // test data generated from `test_generate_bincode`
        let bin = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let res = bincode::deserialize(&bin);
        assert!(res.is_ok());
        let meta: ObjectMetadata = res.unwrap();
        assert_eq!(
            meta.mode(),
            ObjectMode::DIR,
            "Got unexpected mode: {:?}",
            meta.mode()
        );
        assert_eq!(
            meta.last_modified(),
            None,
            "Got unexpected timestamp: {:?}",
            meta.last_modified()
        );
        assert_eq!(
            meta.content_length(),
            0,
            "Got unexpected content_length: {}",
            meta.content_length()
        );
        assert_eq!(
            meta.content_md5(),
            None,
            "Got unexpected content_md5: {:?}",
            meta.content_md5()
        );
        assert_eq!(meta.etag(), None, "Got unexpected etag: {:?}", meta.etag());

        let last_modified = OffsetDateTime::from_unix_timestamp(TIMESTAMP).unwrap();

        // test data generated from `test_generate_bincode`
        let bin = [
            0, 0, 0, 0, 64, 226, 1, 0, 0, 0, 0, 0, 0, 1, 178, 7, 0, 0, 86, 0, 3, 13, 28, 0, 0, 0,
            0, 0, 0, 0, 0,
        ];
        let res = bincode::deserialize(&bin);
        assert!(res.is_ok());
        let meta: ObjectMetadata = res.unwrap();
        assert_eq!(
            meta.mode(),
            ObjectMode::FILE,
            "Got unexpected mode: {:?}",
            meta.mode()
        );
        assert_eq!(
            meta.last_modified(),
            Some(last_modified),
            "Got unexpected timestamp: {:?}",
            meta.last_modified()
        );
        assert_eq!(
            meta.content_length(),
            CONTENT_LENGTH,
            "Got unexpected content_length: {}",
            meta.content_length()
        );
        assert_eq!(
            meta.content_md5(),
            None,
            "Got unexpected content_md5: {:?}",
            meta.content_md5()
        );
        assert_eq!(meta.etag(), None, "Got unexpected etag: {:?}", meta.etag());
    }

    #[test]
    fn test_parse_url() {
        let invalid_scheme = "http://example.com";
        let mut builder = Builder::default();
        builder.endpoint(invalid_scheme);
        let res = builder.build();
        assert!(res.is_err());
    }
}
