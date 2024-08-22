use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

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
