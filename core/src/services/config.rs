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

use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::time::Duration;

/// Config for Aliyun Drive services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct AliyunDriveConfig {
    /// The Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Default to `/` if not set.
    pub root: Option<String>,
    /// The access_token of this backend.
    ///
    /// Solution for client-only purpose. #4733
    ///
    /// Required if no client_id, client_secret and refresh_token are provided.
    pub access_token: Option<String>,
    /// The client_id of this backend.
    ///
    /// Required if no access_token is provided.
    pub client_id: Option<String>,
    /// The client_secret of this backend.
    ///
    /// Required if no access_token is provided.
    pub client_secret: Option<String>,
    /// The refresh_token of this backend.
    ///
    /// Required if no access_token is provided.
    pub refresh_token: Option<String>,
    /// The drive_type of this backend.
    ///
    /// All operations will happen under this type of drive.
    ///
    /// Available values are `default`, `backup` and `resource`.
    ///
    /// Fallback to default if not set or no other drives can be found.
    pub drive_type: String,
}

impl Debug for AliyunDriveConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("AliyunDriveConfig");

        d.field("root", &self.root)
            .field("drive_type", &self.drive_type);

        d.finish_non_exhaustive()
    }
}

/// Config for alluxio services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct AlluxioConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// default to `/` if not set.
    pub root: Option<String>,
    /// endpoint of this backend.
    ///
    /// Endpoint must be full uri, mostly like `http://127.0.0.1:39999`.
    pub endpoint: Option<String>,
}

impl Debug for AlluxioConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("AlluxioConfig");

        d.field("root", &self.root)
            .field("endpoint", &self.endpoint);

        d.finish_non_exhaustive()
    }
}

/// Config for Atomicserver services support
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct AtomicserverConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// private_key of this backend
    pub private_key: Option<String>,
    /// public_key of this backend
    pub public_key: Option<String>,
    /// parent_resource_id of this backend
    pub parent_resource_id: Option<String>,
}

impl Debug for AtomicserverConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicserverConfig")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("public_key", &self.public_key)
            .field("parent_resource_id", &self.parent_resource_id)
            .finish_non_exhaustive()
    }
}

/// Azure Storage Blob services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AzblobConfig {
    /// The root of Azblob service backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,

    /// The container name of Azblob service backend.
    pub container: String,

    /// The endpoint of Azblob service backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azblob: `https://accountname.blob.core.windows.net`
    /// - Azurite: `http://127.0.0.1:10000/devstoreaccount1`
    pub endpoint: Option<String>,

    /// The account name of Azblob service backend.
    pub account_name: Option<String>,

    /// The account key of Azblob service backend.
    pub account_key: Option<String>,

    /// The encryption key of Azblob service backend.
    pub encryption_key: Option<String>,

    /// The encryption key sha256 of Azblob service backend.
    pub encryption_key_sha256: Option<String>,

    /// The encryption algorithm of Azblob service backend.
    pub encryption_algorithm: Option<String>,

    /// The sas token of Azblob service backend.
    pub sas_token: Option<String>,

    /// The maximum batch operations of Azblob service backend.
    pub batch_max_operations: Option<usize>,
}

impl Debug for AzblobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzblobConfig");

        ds.field("root", &self.root);
        ds.field("container", &self.container);
        ds.field("endpoint", &self.endpoint);

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }
        if self.sas_token.is_some() {
            ds.field("sas_token", &"<redacted>");
        }

        ds.finish()
    }
}

/// Azure Data Lake Storage Gen2 Support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AzdlsConfig {
    /// Root of this backend.
    pub root: Option<String>,
    /// Filesystem name of this backend.
    pub filesystem: String,
    /// Endpoint of this backend.
    pub endpoint: Option<String>,
    /// Account name of this backend.
    pub account_name: Option<String>,
    /// Account key of this backend.
    pub account_key: Option<String>,
}

impl Debug for AzdlsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzdlsConfig");

        ds.field("root", &self.root);
        ds.field("filesystem", &self.filesystem);
        ds.field("endpoint", &self.endpoint);

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }

        ds.finish()
    }
}

/// Azure File services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AzfileConfig {
    /// The root path for azfile.
    pub root: Option<String>,
    /// The endpoint for azfile.
    pub endpoint: Option<String>,
    /// The share name for azfile.
    pub share_name: String,
    /// The account name for azfile.
    pub account_name: Option<String>,
    /// The account key for azfile.
    pub account_key: Option<String>,
    /// The sas token for azfile.
    pub sas_token: Option<String>,
}

impl Debug for AzfileConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("AzfileConfig");

        ds.field("root", &self.root);
        ds.field("share_name", &self.share_name);
        ds.field("endpoint", &self.endpoint);

        if self.account_name.is_some() {
            ds.field("account_name", &"<redacted>");
        }
        if self.account_key.is_some() {
            ds.field("account_key", &"<redacted>");
        }
        if self.sas_token.is_some() {
            ds.field("sas_token", &"<redacted>");
        }

        ds.finish()
    }
}

/// Config for backblaze b2 services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct B2Config {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// keyID of this backend.
    ///
    /// - If application_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub application_key_id: Option<String>,
    /// applicationKey of this backend.
    ///
    /// - If application_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub application_key: Option<String>,
    /// bucket of this backend.
    ///
    /// required.
    pub bucket: String,
    /// bucket id of this backend.
    ///
    /// required.
    pub bucket_id: String,
}

impl Debug for B2Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("B2Config");

        d.field("root", &self.root)
            .field("application_key_id", &self.application_key_id)
            .field("bucket_id", &self.bucket_id)
            .field("bucket", &self.bucket);

        d.finish_non_exhaustive()
    }
}

/// cacache service support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CacacheConfig {
    /// That path to the cacache data directory.
    pub datadir: Option<String>,
}

/// Config for Chainsafe services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct ChainsafeConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// api_key of this backend.
    pub api_key: Option<String>,
    /// bucket_id of this backend.
    ///
    /// required.
    pub bucket_id: String,
}

impl Debug for ChainsafeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ChainsafeConfig");

        d.field("root", &self.root)
            .field("bucket_id", &self.bucket_id);

        d.finish_non_exhaustive()
    }
}

/// Cloudflare KV Service Support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CloudflareKvConfig {
    /// The token used to authenticate with CloudFlare.
    pub token: Option<String>,
    /// The account ID used to authenticate with CloudFlare. Used as URI path parameter.
    pub account_id: Option<String>,
    /// The namespace ID. Used as URI path parameter.
    pub namespace_id: Option<String>,

    /// Root within this backend.
    pub root: Option<String>,
}

impl Debug for CloudflareKvConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("CloudflareKvConfig");

        ds.field("root", &self.root);
        ds.field("account_id", &self.account_id);
        ds.field("namespace_id", &self.namespace_id);

        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

/// compio-based file system support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CompfsConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
}

/// Tencent-Cloud COS services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
pub struct CosConfig {
    /// Root of this backend.
    pub root: Option<String>,
    /// Endpoint of this backend.
    pub endpoint: Option<String>,
    /// Secret ID of this backend.
    pub secret_id: Option<String>,
    /// Secret key of this backend.
    pub secret_key: Option<String>,
    /// Bucket of this backend.
    pub bucket: Option<String>,
    /// Disable config load so that opendal will not load config from
    pub disable_config_load: bool,
}

impl Debug for CosConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CosConfig")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("secret_id", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

/// Config for [Cloudflare D1](https://developers.cloudflare.com/d1) backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct D1Config {
    /// Set the token of cloudflare api.
    pub token: Option<String>,
    /// Set the account id of cloudflare api.
    pub account_id: Option<String>,
    /// Set the database id of cloudflare api.
    pub database_id: Option<String>,

    /// Set the working directory of OpenDAL.
    pub root: Option<String>,
    /// Set the table of D1 Database.
    pub table: Option<String>,
    /// Set the key field of D1 Database.
    pub key_field: Option<String>,
    /// Set the value field of D1 Database.
    pub value_field: Option<String>,
}

impl Debug for D1Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("D1Config");
        ds.field("root", &self.root);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.finish_non_exhaustive()
    }
}

/// [dashmap](https://github.com/xacrimon/dashmap) backend support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DashmapConfig {
    /// The root path for dashmap.
    pub root: Option<String>,
}

/// [Dbfs](https://docs.databricks.com/api/azure/workspace/dbfs)'s REST API support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DbfsConfig {
    /// The root for dbfs.
    pub root: Option<String>,
    /// The endpoint for dbfs.
    pub endpoint: Option<String>,
    /// The token for dbfs.
    pub token: Option<String>,
}

impl Debug for DbfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("DbfsConfig");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);

        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

/// Config for [Dropbox](https://www.dropbox.com/) backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct DropboxConfig {
    /// root path for dropbox.
    pub root: Option<String>,
    /// access token for dropbox.
    pub access_token: Option<String>,
    /// refresh_token for dropbox.
    pub refresh_token: Option<String>,
    /// client_id for dropbox.
    pub client_id: Option<String>,
    /// client_secret for dropbox.
    pub client_secret: Option<String>,
}

impl Debug for DropboxConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DropBoxConfig")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// Config for Etcd services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct EtcdConfig {
    /// network address of the Etcd services.
    /// If use https, must set TLS options: `ca_path`, `cert_path`, `key_path`.
    /// e.g. "127.0.0.1:23790,127.0.0.1:23791,127.0.0.1:23792" or "http://127.0.0.1:23790,http://127.0.0.1:23791,http://127.0.0.1:23792" or "https://127.0.0.1:23790,https://127.0.0.1:23791,https://127.0.0.1:23792"
    ///
    /// default is "http://127.0.0.1:2379"
    pub endpoints: Option<String>,
    /// the username to connect etcd service.
    ///
    /// default is None
    pub username: Option<String>,
    /// the password for authentication
    ///
    /// default is None
    pub password: Option<String>,
    /// the working directory of the etcd service. Can be "/path/to/dir"
    ///
    /// default is "/"
    pub root: Option<String>,
    /// certificate authority file path
    ///
    /// default is None
    pub ca_path: Option<String>,
    /// cert path
    ///
    /// default is None
    pub cert_path: Option<String>,
    /// key path
    ///
    /// default is None
    pub key_path: Option<String>,
}

impl Debug for EtcdConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("EtcdConfig");

        ds.field("root", &self.root);
        if let Some(endpoints) = self.endpoints.clone() {
            ds.field("endpoints", &endpoints);
        }
        if let Some(username) = self.username.clone() {
            ds.field("username", &username);
        }
        if self.password.is_some() {
            ds.field("password", &"<redacted>");
        }
        if let Some(ca_path) = self.ca_path.clone() {
            ds.field("ca_path", &ca_path);
        }
        if let Some(cert_path) = self.cert_path.clone() {
            ds.field("cert_path", &cert_path);
        }
        if let Some(key_path) = self.key_path.clone() {
            ds.field("key_path", &key_path);
        }
        ds.finish()
    }
}

/// [foundationdb](https://www.foundationdb.org/) service support.
///Config for FoundationDB.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FoundationdbConfig {
    ///root of the backend.
    pub root: Option<String>,
    ///config_path for the backend.
    pub config_path: Option<String>,
}

impl Debug for FoundationdbConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("FoundationConfig");

        ds.field("root", &self.root);
        ds.field("config_path", &self.config_path);

        ds.finish()
    }
}

/// config for file system
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FsConfig {
    /// root dir for backend
    pub root: Option<String>,

    /// tmp dir for atomic write
    pub atomic_write_dir: Option<String>,
}

/// Config for Ftp services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FtpConfig {
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// root of this backend
    pub root: Option<String>,
    /// user of this backend
    pub user: Option<String>,
    /// password of this backend
    pub password: Option<String>,
}

impl Debug for FtpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpConfig")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// [Google Cloud Storage](https://cloud.google.com/storage) services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GcsConfig {
    /// root URI, all operations happens under `root`
    pub root: Option<String>,
    /// bucket name
    pub bucket: String,
    /// endpoint URI of GCS service,
    /// default is `https://storage.googleapis.com`
    pub endpoint: Option<String>,
    /// Scope for gcs.
    pub scope: Option<String>,
    /// Service Account for gcs.
    pub service_account: Option<String>,
    /// Credentials string for GCS service OAuth2 authentication.
    pub credential: Option<String>,
    /// Local path to credentials file for GCS service OAuth2 authentication.
    pub credential_path: Option<String>,
    /// The predefined acl for GCS.
    pub predefined_acl: Option<String>,
    /// The default storage class used by gcs.
    pub default_storage_class: Option<String>,
    /// Allow opendal to send requests without signing when credentials are not
    /// loaded.
    pub allow_anonymous: bool,
    /// Disable attempting to load credentials from the GCE metadata server when
    /// running within Google Cloud.
    pub disable_vm_metadata: bool,
    /// Disable loading configuration from the environment.
    pub disable_config_load: bool,
    /// A Google Cloud OAuth2 token.
    ///
    /// Takes precedence over `credential` and `credential_path`.
    pub token: Option<String>,
}

impl Debug for GcsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsConfig")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// [GoogleDrive](https://drive.google.com/) configuration.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GdriveConfig {
    /// The root for gdrive
    pub root: Option<String>,
    /// Access token for gdrive.
    pub access_token: Option<String>,
    /// Refresh token for gdrive.
    pub refresh_token: Option<String>,
    /// Client id for gdrive.
    pub client_id: Option<String>,
    /// Client secret for gdrive.
    pub client_secret: Option<String>,
}

impl Debug for GdriveConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GdriveConfig")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// Config for GitHub Action Cache Services support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GhacConfig {
    /// The root path for ghac.
    pub root: Option<String>,
    /// The version that used by cache.
    pub version: Option<String>,
    /// The endpoint for ghac service.
    pub endpoint: Option<String>,
    /// The runtime token for ghac service.
    pub runtime_token: Option<String>,
}

/// Config for GitHub services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GithubConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// GitHub access_token.
    ///
    /// optional.
    /// If not provided, the backend will only support read operations for public repositories.
    /// And rate limit will be limited to 60 requests per hour.
    pub token: Option<String>,
    /// GitHub repo owner.
    ///
    /// required.
    pub owner: String,
    /// GitHub repo name.
    ///
    /// required.
    pub repo: String,
}

impl Debug for GithubConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("GithubConfig");

        d.field("root", &self.root)
            .field("owner", &self.owner)
            .field("repo", &self.repo);

        d.finish_non_exhaustive()
    }
}

/// Config for Grid file system support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GridFsConfig {
    /// The connection string of the MongoDB service.
    pub connection_string: Option<String>,
    /// The database name of the MongoDB GridFs service to read/write.
    pub database: Option<String>,
    /// The bucket name of the MongoDB GridFs service to read/write.
    pub bucket: Option<String>,
    /// The chunk size of the MongoDB GridFs service used to break the user file into chunks.
    pub chunk_size: Option<u32>,
    /// The working directory, all operations will be performed under it.
    pub root: Option<String>,
}

impl Debug for GridFsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GridFsConfig")
            .field("database", &self.database)
            .field("bucket", &self.bucket)
            .field("chunk_size", &self.chunk_size)
            .field("root", &self.root)
            .finish()
    }
}

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
///
/// Config for Hdfs services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HdfsConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// name node of this backend
    pub name_node: Option<String>,
    /// kerberos_ticket_cache_path of this backend
    pub kerberos_ticket_cache_path: Option<String>,
    /// user of this backend
    pub user: Option<String>,
    /// enable the append capacity
    pub enable_append: bool,
    /// atomic_write_dir of this backend
    pub atomic_write_dir: Option<String>,
}

impl Debug for HdfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsConfig")
            .field("root", &self.root)
            .field("name_node", &self.name_node)
            .field(
                "kerberos_ticket_cache_path",
                &self.kerberos_ticket_cache_path,
            )
            .field("user", &self.user)
            .field("enable_append", &self.enable_append)
            .field("atomic_write_dir", &self.atomic_write_dir)
            .finish_non_exhaustive()
    }
}

/// Config for HdfsNative services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HdfsNativeConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// url of this backend
    pub url: Option<String>,
    /// enable the append capacity
    pub enable_append: bool,
}

impl Debug for HdfsNativeConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsNativeConfig")
            .field("root", &self.root)
            .field("url", &self.url)
            .field("enable_append", &self.enable_append)
            .finish_non_exhaustive()
    }
}

/// Config for Http service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HttpConfig {
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// username of this backend
    pub username: Option<String>,
    /// password of this backend
    pub password: Option<String>,
    /// token of this backend
    pub token: Option<String>,
    /// root of this backend
    pub root: Option<String>,
}

impl Debug for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("HttpConfig");
        de.field("endpoint", &self.endpoint);
        de.field("root", &self.root);

        de.finish_non_exhaustive()
    }
}

/// Configuration for Huggingface service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HuggingfaceConfig {
    /// Repo type of this backend. Default is model.
    ///
    /// Available values:
    /// - model
    /// - dataset
    pub repo_type: Option<String>,
    /// Repo id of this backend.
    ///
    /// This is required.
    pub repo_id: Option<String>,
    /// Revision of this backend.
    ///
    /// Default is main.
    pub revision: Option<String>,
    /// Root of this backend. Can be "/path/to/dir".
    ///
    /// Default is "/".
    pub root: Option<String>,
    /// Token of this backend.
    ///
    /// This is optional.
    pub token: Option<String>,
}

impl Debug for HuggingfaceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("HuggingfaceConfig");

        if let Some(repo_type) = &self.repo_type {
            ds.field("repo_type", &repo_type);
        }
        if let Some(repo_id) = &self.repo_id {
            ds.field("repo_id", &repo_id);
        }
        if let Some(revision) = &self.revision {
            ds.field("revision", &revision);
        }
        if let Some(root) = &self.root {
            ds.field("root", &root);
        }
        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

/// Config for icloud services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct IcloudConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// default to `/` if not set.
    pub root: Option<String>,
    /// apple_id of this backend.
    ///
    /// apple_id must be full, mostly like `example@gmail.com`.
    pub apple_id: Option<String>,
    /// password of this backend.
    ///
    /// password must be full.
    pub password: Option<String>,

    /// Session
    ///
    /// token must be valid.
    pub trust_token: Option<String>,
    /// ds_web_auth_token must be set in Session
    pub ds_web_auth_token: Option<String>,
    /// enable the china origin
    /// China region `origin` Header needs to be set to "https://www.icloud.com.cn".
    ///
    /// otherwise Apple server will return 302.
    pub is_china_mainland: bool,
}

impl Debug for IcloudConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("IcloudBuilder");
        d.field("root", &self.root);
        d.field("is_china_mainland", &self.is_china_mainland);
        d.finish_non_exhaustive()
    }
}

/// Config for IPFS file system support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct IpfsConfig {
    /// IPFS gateway endpoint.
    pub endpoint: Option<String>,
    /// IPFS root.
    pub root: Option<String>,
}

/// Config for IPFS MFS support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct IpmfsConfig {
    /// Root for ipfs.
    pub root: Option<String>,
    /// Endpoint for ipfs.
    pub endpoint: Option<String>,
}

/// Config for Koofr services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct KoofrConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// Koofr endpoint.
    pub endpoint: String,
    /// Koofr email.
    pub email: String,
    /// password of this backend. (Must be the application password)
    pub password: Option<String>,
}

impl Debug for KoofrConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);
        ds.field("email", &self.email);

        ds.finish()
    }
}

/// Config for Libsql services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct LibsqlConfig {
    /// Connection string for libsql service.
    pub connection_string: Option<String>,
    /// Authentication token for libsql service.
    pub auth_token: Option<String>,

    /// Table name for libsql service.
    pub table: Option<String>,
    /// Key field name for libsql service.
    pub key_field: Option<String>,
    /// Value field name for libsql service.
    pub value_field: Option<String>,
    /// Root for libsql service.
    pub root: Option<String>,
}

impl Debug for LibsqlConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("LibsqlConfig");
        ds.field("connection_string", &self.connection_string)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root);

        if self.auth_token.is_some() {
            ds.field("auth_token", &"<redacted>");
        }

        ds.finish()
    }
}

/// Config for MemCached services support
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MemcachedConfig {
    /// network address of the memcached service.
    ///
    /// For example: "tcp://localhost:11211"
    pub endpoint: Option<String>,
    /// the working directory of the service. Can be "/path/to/dir"
    ///
    /// default is "/"
    pub root: Option<String>,
    /// Memcached username, optional.
    pub username: Option<String>,
    /// Memcached password, optional.
    pub password: Option<String>,
    /// The default ttl for put operations.
    pub default_ttl: Option<Duration>,
}

/// Config for memory.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MemoryConfig {
    /// root of the backend.
    pub root: Option<String>,
}

/// Config for mini-moka support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MiniMokaConfig {
    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::max_capacity`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.max_capacity)
    pub max_capacity: Option<u64>,
    /// Sets the time to live of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::time_to_live`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_live)
    pub time_to_live: Option<Duration>,
    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    pub time_to_idle: Option<Duration>,

    /// root path of this backend
    pub root: Option<String>,
}

/// Config for Moka services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MokaConfig {
    /// Name for this cache instance.
    pub name: Option<String>,
    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::max_capacity`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.max_capacity)
    pub max_capacity: Option<u64>,
    /// Sets the time to live of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_live`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_live)
    pub time_to_live: Option<Duration>,
    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    pub time_to_idle: Option<Duration>,
    /// Sets the segments number of the cache.
    ///
    /// Refer to [`moka::sync::CacheBuilder::segments`](https://docs.rs/moka/latest/moka/sync/struct.CacheBuilder.html#method.segments)
    pub num_segments: Option<usize>,

    /// root path of this backend
    pub root: Option<String>,
}

impl Debug for MokaConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaConfig")
            .field("name", &self.name)
            .field("max_capacity", &self.max_capacity)
            .field("time_to_live", &self.time_to_live)
            .field("time_to_idle", &self.time_to_idle)
            .field("num_segments", &self.num_segments)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// Config for Mongodb service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MongodbConfig {
    /// connection string of this backend
    pub connection_string: Option<String>,
    /// database of this backend
    pub database: Option<String>,
    /// collection of this backend
    pub collection: Option<String>,
    /// root of this backend
    pub root: Option<String>,
    /// key field of this backend
    pub key_field: Option<String>,
    /// value field of this backend
    pub value_field: Option<String>,
}

impl Debug for MongodbConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongodbConfig")
            .field("connection_string", &self.connection_string)
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("root", &self.root)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish()
    }
}

/// Config for monoiofs services support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MonoiofsConfig {
    /// The Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Builder::build will return error if not set.
    pub root: Option<String>,
}

/// Config for Mysql services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MysqlConfig {
    /// The connection string for mysql.
    pub connection_string: Option<String>,

    /// The table name for mysql.
    pub table: Option<String>,
    /// The key field name for mysql.
    pub key_field: Option<String>,
    /// The value field name for mysql.
    pub value_field: Option<String>,
    /// The root for mysql.
    pub root: Option<String>,
}

impl Debug for MysqlConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("MysqlConfig");

        if self.connection_string.is_some() {
            d.field("connection_string", &"<redacted>");
        }

        d.field("root", &self.root)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish()
    }
}

/// Config for Huawei-Cloud Object Storage Service (OBS) support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct ObsConfig {
    /// Root for obs.
    pub root: Option<String>,
    /// Endpoint for obs.
    pub endpoint: Option<String>,
    /// Access key id for obs.
    pub access_key_id: Option<String>,
    /// Secret access key for obs.
    pub secret_access_key: Option<String>,
    /// Bucket for obs.
    pub bucket: Option<String>,
}

impl Debug for ObsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsConfig")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field("bucket", &self.bucket)
            .finish()
    }
}

/// Config for [OneDrive](https://onedrive.com) backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct OnedriveConfig {
    /// bearer access token for OneDrive
    pub access_token: Option<String>,
    /// root path of OneDrive folder.
    pub root: Option<String>,
}

impl Debug for OnedriveConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnedriveConfig")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// Config for Aliyun Object Storage Service (OSS) support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct OssConfig {
    /// Root for oss.
    pub root: Option<String>,

    /// Endpoint for oss.
    pub endpoint: Option<String>,
    /// Presign endpoint for oss.
    pub presign_endpoint: Option<String>,
    /// Bucket for oss.
    pub bucket: String,

    // OSS features
    /// Server side encryption for oss.
    pub server_side_encryption: Option<String>,
    /// Server side encryption key id for oss.
    pub server_side_encryption_key_id: Option<String>,
    /// Allow anonymous for oss.
    pub allow_anonymous: bool,

    // authenticate options
    /// Access key id for oss.
    pub access_key_id: Option<String>,
    /// Access key secret for oss.
    pub access_key_secret: Option<String>,
    /// batch_max_operations
    pub batch_max_operations: Option<usize>,
}

impl Debug for OssConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Builder");
        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("allow_anonymous", &self.allow_anonymous);

        d.finish_non_exhaustive()
    }
}

/// Config for Pcloud services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct PcloudConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    ///pCloud  endpoint address.
    pub endpoint: String,
    /// pCloud username.
    pub username: Option<String>,
    /// pCloud password.
    pub password: Option<String>,
}

impl Debug for PcloudConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);
        ds.field("username", &self.username);

        ds.finish()
    }
}

/// Config for persy service support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct PersyConfig {
    /// That path to the persy data file. The directory in the path must already exist.
    pub datafile: Option<String>,
    /// That name of the persy segment.
    pub segment: Option<String>,
    /// That name of the persy index.
    pub index: Option<String>,
}

/// Config for PostgreSQL services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct PostgresqlConfig {
    /// Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Default to `/` if not set.
    pub root: Option<String>,
    /// the connection string of postgres server
    pub connection_string: Option<String>,
    /// the table of postgresql
    pub table: Option<String>,
    /// the key field of postgresql
    pub key_field: Option<String>,
    /// the value field of postgresql
    pub value_field: Option<String>,
}

impl Debug for PostgresqlConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("PostgresqlConfig");

        if self.connection_string.is_some() {
            d.field("connection_string", &"<redacted>");
        }

        d.field("root", &self.root)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish()
    }
}

/// Config for redb service support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct RedbConfig {
    /// path to the redb data directory.
    pub datadir: Option<String>,
    /// The root for redb.
    pub root: Option<String>,
    /// The table name for redb.
    pub table: Option<String>,
}

/// Config for Redis services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct RedisConfig {
    /// network address of the Redis service. Can be "tcp://127.0.0.1:6379", e.g.
    ///
    /// default is "tcp://127.0.0.1:6379"
    pub endpoint: Option<String>,
    /// network address of the Redis cluster service. Can be "tcp://127.0.0.1:6379,tcp://127.0.0.1:6380,tcp://127.0.0.1:6381", e.g.
    ///
    /// default is None
    pub cluster_endpoints: Option<String>,
    /// the username to connect redis service.
    ///
    /// default is None
    pub username: Option<String>,
    /// the password for authentication
    ///
    /// default is None
    pub password: Option<String>,
    /// the working directory of the Redis service. Can be "/path/to/dir"
    ///
    /// default is "/"
    pub root: Option<String>,
    /// the number of DBs redis can take is unlimited
    ///
    /// default is db 0
    pub db: i64,
    /// The default ttl for put operations.
    pub default_ttl: Option<Duration>,
}

impl Debug for RedisConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RedisConfig");

        d.field("db", &self.db.to_string());
        d.field("root", &self.root);
        if let Some(endpoint) = self.endpoint.clone() {
            d.field("endpoint", &endpoint);
        }
        if let Some(cluster_endpoints) = self.cluster_endpoints.clone() {
            d.field("cluster_endpoints", &cluster_endpoints);
        }
        if let Some(username) = self.username.clone() {
            d.field("username", &username);
        }
        if self.password.is_some() {
            d.field("password", &"<redacted>");
        }

        d.finish_non_exhaustive()
    }
}

/// Config for Rocksdb Service.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct RocksdbConfig {
    /// The path to the rocksdb data directory.
    pub datadir: Option<String>,
    /// the working directory of the service. Can be "/path/to/dir"
    ///
    /// default is "/"
    pub root: Option<String>,
}

/// Config for Aws S3 and compatible services (including minio, digitalocean space, Tencent Cloud Object Storage(COS) and so on) support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct S3Config {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// default to `/` if not set.
    pub root: Option<String>,
    /// bucket name of this backend.
    ///
    /// required.
    pub bucket: String,
    /// endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - AWS S3: `https://s3.amazonaws.com` or `https://s3.{region}.amazonaws.com`
    /// - Cloudflare R2: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
    /// - Aliyun OSS: `https://{region}.aliyuncs.com`
    /// - Tencent COS: `https://cos.{region}.myqcloud.com`
    /// - Minio: `http://127.0.0.1:9000`
    ///
    /// If user inputs endpoint without scheme like "s3.amazonaws.com", we
    /// will prepend "https://" before it.
    ///
    /// default to `https://s3.amazonaws.com` if not set.
    pub endpoint: Option<String>,
    /// Region represent the signing region of this endpoint. This is required
    /// if you are using the default AWS S3 endpoint.
    ///
    /// If using a custom endpoint,
    /// - If region is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub region: Option<String>,

    /// access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub access_key_id: Option<String>,
    /// secret_access_key of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub secret_access_key: Option<String>,
    /// session_token (aka, security token) of this backend.
    ///
    /// This token will expire after sometime, it's recommended to set session_token
    /// by hand.
    pub session_token: Option<String>,
    /// role_arn for this backend.
    ///
    /// If `role_arn` is set, we will use already known config as source
    /// credential to assume role with `role_arn`.
    pub role_arn: Option<String>,
    /// external_id for this backend.
    pub external_id: Option<String>,
    /// role_session_name for this backend.
    pub role_session_name: Option<String>,
    /// Disable config load so that opendal will not load config from
    /// environment.
    ///
    /// For examples:
    ///
    /// - envs like `AWS_ACCESS_KEY_ID`
    /// - files like `~/.aws/config`
    pub disable_config_load: bool,
    /// Disable load credential from ec2 metadata.
    ///
    /// This option is used to disable the default behavior of opendal
    /// to load credential from ec2 metadata, a.k.a, IMDSv2
    pub disable_ec2_metadata: bool,
    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    pub allow_anonymous: bool,
    /// server_side_encryption for this backend.
    ///
    /// Available values: `AES256`, `aws:kms`.
    pub server_side_encryption: Option<String>,
    /// server_side_encryption_aws_kms_key_id for this backend
    ///
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is not set, S3 will use aws managed kms key to encrypt data.
    /// - If `server_side_encryption` set to `aws:kms`, and `server_side_encryption_aws_kms_key_id`
    ///   is a valid kms key id, S3 will use the provided kms key to encrypt data.
    /// - If the `server_side_encryption_aws_kms_key_id` is invalid or not found, an error will be
    ///   returned.
    /// - If `server_side_encryption` is not `aws:kms`, setting `server_side_encryption_aws_kms_key_id`
    ///   is a noop.
    pub server_side_encryption_aws_kms_key_id: Option<String>,
    /// server_side_encryption_customer_algorithm for this backend.
    ///
    /// Available values: `AES256`.
    pub server_side_encryption_customer_algorithm: Option<String>,
    /// server_side_encryption_customer_key for this backend.
    ///
    /// # Value
    ///
    /// base64 encoded key that matches algorithm specified in
    /// `server_side_encryption_customer_algorithm`.
    pub server_side_encryption_customer_key: Option<String>,
    /// Set server_side_encryption_customer_key_md5 for this backend.
    ///
    /// # Value
    ///
    /// MD5 digest of key specified in `server_side_encryption_customer_key`.
    pub server_side_encryption_customer_key_md5: Option<String>,
    /// default storage_class for this backend.
    ///
    /// Available values:
    /// - `DEEP_ARCHIVE`
    /// - `GLACIER`
    /// - `GLACIER_IR`
    /// - `INTELLIGENT_TIERING`
    /// - `ONEZONE_IA`
    /// - `OUTPOSTS`
    /// - `REDUCED_REDUNDANCY`
    /// - `STANDARD`
    /// - `STANDARD_IA`
    ///
    /// S3 compatible services don't support all of them
    pub default_storage_class: Option<String>,
    /// Enable virtual host style so that opendal will send API requests
    /// in virtual host style instead of path style.
    ///
    /// - By default, opendal will send API to `https://s3.us-east-1.amazonaws.com/bucket_name`
    /// - Enabled, opendal will send API to `https://bucket_name.s3.us-east-1.amazonaws.com`
    pub enable_virtual_host_style: bool,
    /// Set maximum batch operations of this backend.
    ///
    /// Some compatible services have a limit on the number of operations in a batch request.
    /// For example, R2 could return `Internal Error` while batch delete 1000 files.
    ///
    /// Please tune this value based on services' document.
    pub batch_max_operations: Option<usize>,
    /// Disable stat with override so that opendal will not send stat request with override queries.
    ///
    /// For example, R2 doesn't support stat with `response_content_type` query.
    pub disable_stat_with_override: bool,
    /// Checksum Algorithm to use when sending checksums in HTTP headers.
    /// This is necessary when writing to AWS S3 Buckets with Object Lock enabled for example.
    ///
    /// Available options:
    /// - "crc32c"
    pub checksum_algorithm: Option<String>,
}

impl Debug for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("S3Config");

        d.field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region);

        d.finish_non_exhaustive()
    }
}

/// Config for seafile services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SeafileConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// endpoint address of this backend.
    pub endpoint: Option<String>,
    /// username of this backend.
    pub username: Option<String>,
    /// password of this backend.
    pub password: Option<String>,
    /// repo_name of this backend.
    ///
    /// required.
    pub repo_name: String,
}

impl Debug for SeafileConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SeafileConfig");

        d.field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .field("repo_name", &self.repo_name);

        d.finish_non_exhaustive()
    }
}

/// Config for Sftp Service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SftpConfig {
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// root of this backend
    pub root: Option<String>,
    /// user of this backend
    pub user: Option<String>,
    /// key of this backend
    pub key: Option<String>,
    /// known_hosts_strategy of this backend
    pub known_hosts_strategy: Option<String>,
    /// enable_copy of this backend
    pub enable_copy: bool,
}

impl Debug for SftpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpConfig")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

/// Config for Sled services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SledConfig {
    /// That path to the sled data directory.
    pub datadir: Option<String>,
    /// The root for sled.
    pub root: Option<String>,
    /// The tree for sled.
    pub tree: Option<String>,
}

impl Debug for SledConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SledConfig")
            .field("datadir", &self.datadir)
            .field("root", &self.root)
            .field("tree", &self.tree)
            .finish()
    }
}

/// Config for Sqlite support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SqliteConfig {
    /// Set the connection_string of the sqlite service.
    ///
    /// This connection string is used to connect to the sqlite service. There are url based formats:
    ///
    /// ## Url
    ///
    /// This format resembles the url format of the sqlite client. The format is: `file://[path]?flag`
    ///
    /// - `file://data.db`
    ///
    /// For more information, please refer to [Opening A New Database Connection](http://www.sqlite.org/c3ref/open.html)
    pub connection_string: Option<String>,

    /// Set the table name of the sqlite service to read/write.
    pub table: Option<String>,
    /// Set the key field name of the sqlite service to read/write.
    ///
    /// Default to `key` if not specified.
    pub key_field: Option<String>,
    /// Set the value field name of the sqlite service to read/write.
    ///
    /// Default to `value` if not specified.
    pub value_field: Option<String>,
    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub root: Option<String>,
}

impl Debug for SqliteConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SqliteConfig");

        d.field("connection_string", &self.connection_string)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root);

        d.finish_non_exhaustive()
    }
}

/// Config for supabase service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SupabaseConfig {
    /// The root for supabase service.
    pub root: Option<String>,
    /// The bucket for supabase service.
    pub bucket: String,
    /// The endpoint for supabase service.
    pub endpoint: Option<String>,
    /// The key for supabase service.
    pub key: Option<String>,
    // TODO(1) optional public, currently true always
    // TODO(2) optional file_size_limit, currently 0
    // TODO(3) optional allowed_mime_types, currently only string
}

impl Debug for SupabaseConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupabaseConfig")
            .field("root", &self.root)
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

/// Config for Surrealdb services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SurrealdbConfig {
    /// The connection string for surrealdb.
    pub connection_string: Option<String>,
    /// The username for surrealdb.
    pub username: Option<String>,
    /// The password for surrealdb.
    pub password: Option<String>,
    /// The namespace for surrealdb.
    pub namespace: Option<String>,
    /// The database for surrealdb.
    pub database: Option<String>,
    /// The table for surrealdb.
    pub table: Option<String>,
    /// The key field for surrealdb.
    pub key_field: Option<String>,
    /// The value field for surrealdb.
    pub value_field: Option<String>,
    /// The root for surrealdb.
    pub root: Option<String>,
}

impl Debug for SurrealdbConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SurrealdbConfig");

        d.field("connection_string", &self.connection_string)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("namespace", &self.namespace)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root)
            .finish()
    }
}

/// Config for OpenStack Swift support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SwiftConfig {
    /// The endpoint for Swift.
    pub endpoint: Option<String>,
    /// The container for Swift.
    pub container: Option<String>,
    /// The root for Swift.
    pub root: Option<String>,
    /// The token for Swift.
    pub token: Option<String>,
}

impl Debug for SwiftConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("SwiftConfig");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);
        ds.field("container", &self.container);

        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

/// Config for Tikv services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct TikvConfig {
    /// network address of the TiKV service.
    pub endpoints: Option<Vec<String>>,
    /// whether using insecure connection to TiKV
    pub insecure: bool,
    /// certificate authority file path
    pub ca_path: Option<String>,
    /// cert path
    pub cert_path: Option<String>,
    /// key path
    pub key_path: Option<String>,
}

impl Debug for TikvConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("TikvConfig");

        d.field("endpoints", &self.endpoints)
            .field("insecure", &self.insecure)
            .field("ca_path", &self.ca_path)
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .finish()
    }
}

/// Config for upyun services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct UpyunConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// bucket address of this backend.
    pub bucket: String,
    /// username of this backend.
    pub operator: Option<String>,
    /// password of this backend.
    pub password: Option<String>,
}

impl Debug for UpyunConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);
        ds.field("bucket", &self.bucket);
        ds.field("operator", &self.operator);

        ds.finish()
    }
}

/// Config for Vercel Cache support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct VercelArtifactsConfig {
    /// The access token for Vercel.
    pub access_token: Option<String>,
}

impl Debug for VercelArtifactsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelArtifactsConfig")
            .field("access_token", &"<redacted>")
            .finish()
    }
}

/// Config for VercelBlob services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct VercelBlobConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// vercel blob token.
    pub token: String,
}

impl Debug for VercelBlobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);

        ds.finish()
    }
}

/// Config for [WebDAV](https://datatracker.ietf.org/doc/html/rfc4918) backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct WebdavConfig {
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// username of this backend
    pub username: Option<String>,
    /// password of this backend
    pub password: Option<String>,
    /// token of this backend
    pub token: Option<String>,
    /// root of this backend
    pub root: Option<String>,
    /// WebDAV Service doesn't support copy.
    pub disable_copy: bool,
}

impl Debug for WebdavConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("WebdavConfig");

        d.field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .field("root", &self.root);

        d.finish_non_exhaustive()
    }
}

/// Config for WebHDFS support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct WebhdfsConfig {
    /// Root for webhdfs.
    pub root: Option<String>,
    /// Endpoint for webhdfs.
    pub endpoint: Option<String>,
    /// Delegation token for webhdfs.
    pub delegation: Option<String>,
    /// Disable batch listing
    pub disable_list_batch: bool,
    /// atomic_write_dir of this backend
    pub atomic_write_dir: Option<String>,
}

impl Debug for WebhdfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhdfsConfig")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("atomic_write_dir", &self.atomic_write_dir)
            .finish_non_exhaustive()
    }
}

/// Config for YandexDisk services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct YandexDiskConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
    /// yandex disk oauth access_token.
    pub access_token: String,
}

impl Debug for YandexDiskConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Config");

        ds.field("root", &self.root);

        ds.finish()
    }
}
