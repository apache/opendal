use crate::error::{other, BackendError};
use crate::Accessor;
use anyhow::anyhow;
use redis::{Client, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::Result;
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
                let port = ep_url.port().unwrap_or_else(|| 6379);
                ConnectionAddr::Tcp(host, port)
            }
            "tcps" | "rediss" => {
                let host = ep_url
                    .host()
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                let port = ep_url.port().unwrap_or_else(|| 6379);
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

impl Accessor for Backend {}
