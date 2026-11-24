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

use std::collections::HashMap;
use std::sync::Arc;

use log::debug;

use super::HDFS_NATIVE_SCHEME;
use super::config::HDFS_DEFAULT_AUTHORITY;
use super::config::HDFS_SCHEME_PREFIX;
use super::config::HdfsNativeConfig;
use super::config::init_hdfs_config;
use super::core::HdfsNativeCore;
use super::core::parse_hdfs_error;
use super::deleter::HdfsNativeDeleter;
use super::reader::*;
use opendal_core::raw::*;
use opendal_core::*;

/// [Hadoop Distributed File System (HDFS™)](https://hadoop.apache.org/) support.
/// Using [Native Rust HDFS client](https://github.com/Kimahriman/hdfs-native).
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct HdfsNativeBuilder {
    pub(super) config: HdfsNativeConfig,
}

impl HdfsNativeBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set name_node of this backend.
    ///
    /// Valid format including:
    ///
    /// - `default`: using the default setting based on hadoop config.
    /// - `hdfs://127.0.0.1:9000`: connect to hdfs cluster.
    /// - `hdfs://namenode1:9000,namenode2:9000`: connect to hdfs ha cluster.
    pub fn name_node(mut self, name_node: &str) -> Self {
        if !name_node.is_empty() {
            // Trim trailing `/` so that we can accept `hdfs://127.0.0.1:9000/`
            // without stripping the `://` delimiter from scheme-only inputs like `hdfs://`.
            let normalized = if let Some((scheme, rest)) = name_node.split_once("://") {
                format!("{scheme}://{}", rest.trim_end_matches('/'))
            } else {
                name_node.trim_end_matches('/').to_string()
            };
            self.config.name_node = Some(normalized)
        }

        self
    }

    /// Deprecated: HDFS Native append capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "HDFS Native append capability is enabled by default and this option is no longer needed."
    )]
    pub fn enable_append(self, _enable_append: bool) -> Self {
        self
    }

    /// Set other hdfs-native client options of this backend.
    ///
    /// See the [hdfs-native documentation](https://github.com/Kimahriman/hdfs-native)
    /// for the supported options.
    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.config.options = Some(options);
        self
    }
}

impl Builder for HdfsNativeBuilder {
    type Config = HdfsNativeConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let name_node = match self.config.name_node.as_deref().map(str::trim) {
            Some(v) if !v.is_empty() => v,
            _ => {
                return Err(Error::new(ErrorKind::ConfigInvalid, "name_node is empty")
                    .with_context("service", HDFS_NATIVE_SCHEME));
            }
        };

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let mut client_builder = hdfs_native::ClientBuilder::new();
        if name_node == "default" {
            if let Some(options) = self.config.options {
                client_builder = client_builder.with_config(options);
            }
        } else if name_node.contains(',') {
            let mut hdfs_config = init_hdfs_config(name_node);
            if hdfs_config.is_empty() {
                return Err(Error::new(ErrorKind::ConfigInvalid, "name_node is empty")
                    .with_context("service", HDFS_NATIVE_SCHEME));
            }
            if let Some(options) = self.config.options {
                hdfs_config.extend(options);
            }
            client_builder = client_builder
                .with_url(format!("{HDFS_SCHEME_PREFIX}{HDFS_DEFAULT_AUTHORITY}"))
                .with_config(hdfs_config);
        } else {
            let url = if let Some((scheme, rest)) = name_node.split_once("://") {
                let authority = rest.trim().trim_end_matches('/');
                if authority.is_empty() || authority.starts_with('/') {
                    return Err(Error::new(ErrorKind::ConfigInvalid, "name_node is empty")
                        .with_context("service", HDFS_NATIVE_SCHEME));
                }
                format!("{scheme}://{authority}")
            } else {
                let authority = name_node.trim_end_matches('/');
                if authority.is_empty() || authority.eq_ignore_ascii_case("hdfs:") {
                    return Err(Error::new(ErrorKind::ConfigInvalid, "name_node is empty")
                        .with_context("service", HDFS_NATIVE_SCHEME));
                }
                format!("{HDFS_SCHEME_PREFIX}{authority}")
            };

            client_builder = client_builder.with_url(url);
            if let Some(options) = self.config.options {
                client_builder = client_builder.with_config(options);
            }
        }

        let client = client_builder.build().map_err(|err| match &err {
            hdfs_native::HdfsError::InvalidArgument(_)
            | hdfs_native::HdfsError::UrlParseError(_)
            | hdfs_native::HdfsError::XmlParseError(_) => {
                Error::new(ErrorKind::ConfigInvalid, err.to_string())
                    .with_context("service", HDFS_NATIVE_SCHEME)
                    .set_source(err)
            }
            _ => parse_hdfs_error(err),
        })?;

        // need to check if root dir exists, create if not
        Ok(HdfsNativeBackend {
            core: Arc::new(HdfsNativeCore {
                info: ServiceInfo::new(HDFS_NATIVE_SCHEME, &root, ""),
                capability: Capability {
                    stat: true,

                    read: true,

                    write: true,
                    write_can_append: true,

                    create_dir: true,
                    delete: true,
                    delete_with_recursive: true,

                    list: true,

                    rename: true,
                    rename_with_if_not_exists: true,

                    shared: true,

                    ..Default::default()
                },
                root,
                client: Arc::new(client),
            }),
        })
    }
}

// #[inline]
// fn tmp_file_of(path: &str) -> String {
//     let name = get_basename(path);
//     let uuid = Uuid::new_v4().to_string();

//     format!("{name}.{uuid}")
// }

/// Backend for hdfs-native services.
#[derive(Debug, Clone)]
pub struct HdfsNativeBackend {
    pub(crate) core: Arc<HdfsNativeCore>,
}

impl Service for HdfsNativeBackend {
    type Reader = HdfsNativeReader;
    type Writer = HdfsNativeLazyWriter;
    type Lister = HdfsNativeLazyLister;
    type Deleter = oio::OneShotDeleter<HdfsNativeDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.core.hdfs_create_dir(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _args: OpStat) -> Result<RpStat> {
        let m = self.core.hdfs_stat(path).await?;
        Ok(RpStat::new(m))
    }
    fn read(&self, _ctx: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(HdfsNativeReader::new(self.core.clone(), path))
    }

    fn write(&self, _ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(HdfsNativeLazyWriter::new(self.core.clone(), path, args))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<HdfsNativeDeleter> = {
            Ok(oio::OneShotDeleter::new(HdfsNativeDeleter::new(
                Arc::clone(&self.core),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, _args: OpList) -> Result<Self::Lister> {
        Ok(HdfsNativeLazyLister::new(self.core.clone(), path))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.core.hdfs_rename(from, to, &args).await?;
        Ok(RpRename::default())
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_rejects_empty_and_empty_host_name_node() {
        for invalid in [
            None,
            Some(""),
            Some("   "),
            Some("hdfs://"),
            Some("hdfs:///"),
            Some("hdfs:///user/data"),
            Some("hdfs://:9000"),
            Some("hdfs:"),
            Some("viewfs://"),
            Some(","),
            Some("hdfs://,hdfs://"),
        ] {
            #[allow(deprecated)]
            let config = HdfsNativeConfig {
                name_node: invalid.map(str::to_string),
                ..Default::default()
            };
            let err = config.into_builder().build().expect_err("must fail");
            assert_eq!(
                err.kind(),
                ErrorKind::ConfigInvalid,
                "invalid name_node {invalid:?} returned {err:?}"
            );
        }

        let err = HdfsNativeBuilder::default()
            .name_node("hdfs://")
            .build()
            .expect_err("must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn build_supports_default_name_node_from_options() {
        let options = HashMap::from([(
            "fs.defaultFS".to_string(),
            "hdfs://127.0.0.1:9000".to_string(),
        )]);
        let service = HdfsNativeBuilder::default()
            .name_node("default")
            .options(options)
            .build();
        assert!(
            service.is_ok(),
            "default name_node should resolve fs.defaultFS: {:?}",
            service.err()
        );

        let ha_options = HashMap::from([
            ("fs.defaultFS".to_string(), "hdfs://mycluster".to_string()),
            ("dfs.ha.namenodes.mycluster".to_string(), "nn1".to_string()),
            (
                "dfs.namenode.rpc-address.mycluster.nn1".to_string(),
                "127.0.0.1:9000".to_string(),
            ),
        ]);
        let ha_service = HdfsNativeBuilder::default()
            .name_node("default")
            .options(ha_options)
            .build();
        assert!(
            ha_service.is_ok(),
            "default name_node should resolve HA fs.defaultFS: {:?}",
            ha_service.err()
        );
    }

    #[test]
    fn build_supports_logical_ha_nameservice() {
        let options = HashMap::from([
            (
                "dfs.ha.namenodes.mycluster".to_string(),
                "nn1,nn2".to_string(),
            ),
            (
                "dfs.namenode.rpc-address.mycluster.nn1".to_string(),
                "127.0.0.1:9000".to_string(),
            ),
            (
                "dfs.namenode.rpc-address.mycluster.nn2".to_string(),
                "127.0.0.1:9001".to_string(),
            ),
        ]);

        let service = HdfsNativeBuilder::default()
            .name_node("hdfs://mycluster")
            .options(options)
            .build();
        assert!(
            service.is_ok(),
            "logical HA nameservice should build: {:?}",
            service.err()
        );

        let literal_nameservice_options = HashMap::from([
            (
                "dfs.ha.namenodes.nameservice".to_string(),
                "a,b".to_string(),
            ),
            (
                "dfs.namenode.rpc-address.nameservice.a".to_string(),
                "127.0.0.1:9000".to_string(),
            ),
            (
                "dfs.namenode.rpc-address.nameservice.b".to_string(),
                "127.0.0.1:9001".to_string(),
            ),
        ]);
        assert!(
            HdfsNativeBuilder::default()
                .name_node("hdfs://nameservice")
                .options(literal_nameservice_options)
                .build()
                .is_ok()
        );

        let err = HdfsNativeBuilder::default()
            .name_node("hdfs://unconfigured-cluster")
            .build()
            .expect_err("unconfigured nameservice must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn build_supports_comma_separated_ha_namenodes_and_single_endpoint() {
        assert!(
            HdfsNativeBuilder::default()
                .name_node("hdfs://127.0.0.1:9000")
                .build()
                .is_ok()
        );
        assert!(
            HdfsNativeBuilder::default()
                .name_node("127.0.0.1:9000")
                .build()
                .is_ok()
        );
        assert!(
            HdfsNativeBuilder::default()
                .name_node("hdfs://127.0.0.1:9000,127.0.0.1:9001")
                .build()
                .is_ok()
        );
        assert!(
            HdfsNativeBuilder::default()
                .name_node("hdfs://127.0.0.1:9000,hdfs://127.0.0.1:9001/")
                .build()
                .is_ok()
        );
    }

    #[test]
    fn build_from_uri_supports_endpoint_and_logical_nameservice() {
        let endpoint_uri = OperatorUri::new(
            "hdfs-native://127.0.0.1:9000/user/project",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let cfg = HdfsNativeConfig::from_uri(&endpoint_uri).unwrap();
        assert!(cfg.into_builder().build().is_ok());

        let ha_uri = OperatorUri::new(
            "hdfs-native://mycluster/user/project",
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let mut cfg = HdfsNativeConfig::from_uri(&ha_uri).unwrap();
        cfg.options = Some(HashMap::from([
            ("dfs.ha.namenodes.mycluster".to_string(), "nn1".to_string()),
            (
                "dfs.namenode.rpc-address.mycluster.nn1".to_string(),
                "127.0.0.1:9000".to_string(),
            ),
        ]));
        assert!(cfg.into_builder().build().is_ok());
    }
}
