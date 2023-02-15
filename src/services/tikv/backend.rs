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

use core::fmt::Debug;
use std::{collections::HashMap, path::PathBuf, time::Duration};

use tikv_client::{Config, RawClient};
use tokio::sync::OnceCell;

use crate::raw::*;
use crate::*;
use crate::{raw::adapters::kv, Builder};

/// [TiKV](https://tikv.org/) services support.
///
/// # Capabilities
///
/// This service can be used for:
///
/// - [x] read
/// - [x] write
/// - [ ] ~~list~~
/// - [ ] scan
/// - [ ] ~~presign~~
/// - [ ] ~~multipart~~
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the working directory of `OpenDAL`
/// - `endpoints`: Set the PD endpoints for TiKV client
/// - `timeout`: Set the connection timeout for TiKV client
/// - `ca_path`: Set the path to certificate authority
/// - `cert_path`: Set the path to certificate
/// - `key_path`: Set the path to key
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use std::time::Duration;
///
/// use anyhow::Result;
/// use opendal::services::Tikv;
/// use opendal::Object;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Tikv::default();
///     // set the pd endpoints to builder
///     // default to "127.0.0.1:2379" if no endpoints provided
///     builder.endpoints(vec!["127.0.0.1:2378"]);
///     // appending pd endpoint to builder is also supported
///     builder.add_endpoint("127.0.0.1:2379");
///     // set the working directory
///     // default to "/" if not provided
///     builder.root("/tmp/opendal");
///     // set the timeout
///     // default to 2 secs if not provided
///     builder.timeout(Duration::from_secs(2));
///     // set up TLS
///     // use raw unsecured connection if not provided
///     builder.ca_path("path/to/ca");
///     builder.cert_path("path/to/cert");
///     builder.key_path("path/to/key");
///     // build backend
///     let op: Operator = Operator::create(builder)?.finish();
///     let _: Object = op.object("hello-world.txt");
///     Ok(())
/// }
/// ```
#[derive(Clone, Default)]
pub struct TikvBuilder {
    /// addresses of TiKV PD service, can be multiple
    ///
    /// if empty, will offer "127.0.0.1:2379" as default
    endpoints: Vec<String>,
    /// the working directory
    ///
    /// default: "/"
    root: Option<String>,
    ///
    /// path to certification authority
    ca_path: Option<PathBuf>,
    /// path to certification
    cert_path: Option<PathBuf>,
    /// path to key
    key_path: Option<PathBuf>,
    /// timeout
    ///
    /// default is 2 secs
    timeout: Option<Duration>,
}

impl Debug for TikvBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");
        ds.field("root", &self.root);
        ds.field("endpoints", &self.endpoints);
        ds.field("timeout", &self.timeout);

        if self.ca_path.is_some() {
            ds.field("ca_path", &"<redacted>");
        }
        if self.cert_path.is_some() {
            ds.field("cert_path", &"<redacted>");
        }
        if self.key_path.is_some() {
            ds.field("key_path", &"<redacted>");
        }
        ds.finish()
    }
}

impl TikvBuilder {
    /// Set the endpoints of TiKV PD service
    ///
    /// This will replace all existing endpoints in builder with given vector
    /// If no endpoints provided, will use "127.0.0.1:2379"
    pub fn endpoints(&mut self, endpoints: Vec<&str>) -> &mut Self {
        if !endpoints.is_empty() {
            self.endpoints = endpoints;
        }
        self
    }

    /// Add a endpoint of TiKV PD service
    ///
    /// If no endpoints provided, will use "127.0.0.1:2379"
    pub fn add_endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            self.endpoints.push(endpoint.to_string());
        }
        self
    }

    /// set the timeout
    ///
    /// If not set, will be 2 secs
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    /// set the working directory
    ///
    /// if not set, will be "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_owned());
        }
        self
    }

    /// set the ca_path
    pub fn ca_path(&mut self, ca_path: impl Into<PathBuf>) -> &mut Self {
        if !ca_path.is_empty() {
            self.ca_path = Some(ca_path.into());
        }
        self
    }

    /// set the cert_path
    pub fn cert_path(&mut self, cert_path: impl Into<PathBuf>) -> &mut Self {
        if !cert_path.is_empty() {
            self.cert_path = Some(cert_path.into());
        }
        self
    }

    /// set the key_path
    pub fn key_path(&mut self, key_path: impl Into<PathBuf>) -> &mut Self {
        if !key_path.is_empty() {
            self.key_path = Some(key_path.into());
        }
        self
    }
}

impl Builder for TikvBuilder {
    const SCHEME: crate::Scheme = Scheme::Tikv;
    type Accessor = TikvBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = TikvBuilder::default();

        map.get("root").map(|root| builder.root(root));
        map.get("endpoints")
            .map(|endpoints| builder.endpoints(endpoints.split(',').collect()));
        map.get("timeout")
            .map(|timeout| builder.timeout(Duration::from_secs(timeout.parse().unwrap())));
        map.get("ca_path").map(|ca_path| builder.ca_path(ca_path));
        map.get("cert_path")
            .map(|cert_path| builder.cert_path(cert_path));
        map.get("key_path")
            .map(|key_path| builder.key_path(key_path));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let endpoints = if !self.endpoints.is_empty() {
            self.endpoints.clone()
        } else {
            vec!["127.0.0.1:2379"]
        };

        let root = if let Some(root) = &self.root {
            root.clone()
        } else {
            "/".to_owned()
        };

        let timeout = if let Some(timeout) = &self.timeout {
            *timeout
        } else {
            Duration::from_secs(2)
        };

        let mut config = Config::default().with_timeout(timeout);
        if let (Some(ca_path), Some(cert_path), Some(key_path)) = (
            self.ca_path.clone(),
            self.cert_path.clone(),
            self.key_path.clone(),
        ) {
            config = config.with_security(ca_path, cert_path, key_path);
        }

        let backend = TikvAdapter {
            endpoints,
            config,
            client: OnceCell::new(),
        };

        Ok(TikvBackend::new(backend).with_root(&root))
    }
}

/// Backend for tikv
pub type TikvBackend = kv::Backend<TikvAdapter>;

#[derive(Clone)]
pub struct TikvAdapter {
    endpoints: Vec<String>,
    config: Config,
    client: OnceCell<RawClient>,
}

impl Debug for TikvAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TikvAdapter")
            .field("endpoints", &self.endpoints)
            .field("config", &self.config)
            .finish()
    }
}

impl TikvAdapter {
    async fn client(&self) -> Result<&RawClient> {
        self.client.get_or_try_init(|| {
            RawClient::new_with_config(self.endpoints.clone(), self.config.clone())
        })
    }
}

#[async_trait]
impl kv::Adapter for TikvAdapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Redis,
            &self.client.get_connection_info().addr.to_string(),
            AccessorCapability::Read | AccessorCapability::Write,
        )
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut client = self.client().await?;
        let bs: Option<Vec<u8>> = client.get(key).await?;
        Ok(bs)
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut client = self.client().await?;
        let _: () = client.put(key, value).await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.client().await?;
        let _: () = client.delete(key).await?;
        Ok(())
    }
}
