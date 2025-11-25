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

use std::sync::Arc;

use crate::layers::*;
use crate::raw::*;
use crate::types::IntoOperatorUri;
use crate::*;

/// # Operator build API
///
/// Operator should be built via [`OperatorBuilder`]. We recommend to use [`Operator::new`] to get started:
///
/// ```
/// # use anyhow::Result;
/// use opendal::services::Fs;
/// use opendal::Operator;
/// async fn test() -> Result<()> {
///     // Create fs backend builder.
///     let builder = Fs::default().root("/tmp");
///
///     // Build an `Operator` to start operating the storage.
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
impl Operator {
    /// Create a new operator with input builder.
    ///
    /// OpenDAL will call `builder.build()` internally, so we don't need
    /// to import `opendal::Builder` trait.
    ///
    /// # Examples
    ///
    /// Read more backend init examples in [examples](https://github.com/apache/opendal/tree/main/examples).
    ///
    /// ```
    /// # use anyhow::Result;
    /// use opendal::services::Fs;
    /// use opendal::Operator;
    /// async fn test() -> Result<()> {
    ///     // Create fs backend builder.
    ///     let builder = Fs::default().root("/tmp");
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::new(builder)?.finish();
    ///
    ///     Ok(())
    /// }
    /// ```
    #[allow(clippy::new_ret_no_self)]
    pub fn new<B: Builder>(ab: B) -> Result<OperatorBuilder<impl Access>> {
        let acc = ab.build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator from given config.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// use std::collections::HashMap;
    ///
    /// use opendal::services::MemoryConfig;
    /// use opendal::Operator;
    /// async fn test() -> Result<()> {
    ///     let cfg = MemoryConfig::default();
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::from_config(cfg)?.finish();
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn from_config<C: Configurator>(cfg: C) -> Result<OperatorBuilder<impl Access>> {
        let builder = cfg.into_builder();
        let acc = builder.build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator from given iterator in static dispatch.
    ///
    /// # Notes
    ///
    /// `from_iter` generates a `OperatorBuilder` which allows adding layer in zero-cost way.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// use std::collections::HashMap;
    ///
    /// use opendal::services::Fs;
    /// use opendal::Operator;
    /// async fn test() -> Result<()> {
    ///     let map = HashMap::from([
    ///         // Set the root for fs, all operations will happen under this root.
    ///         //
    ///         // NOTE: the root must be absolute path.
    ///         ("root".to_string(), "/tmp".to_string()),
    ///     ]);
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::from_iter::<Fs>(map)?.finish();
    ///
    ///     Ok(())
    /// }
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<B: Builder>(
        iter: impl IntoIterator<Item = (String, String)>,
    ) -> Result<OperatorBuilder<impl Access>> {
        let builder = B::Config::from_iter(iter)?.into_builder();
        let acc = builder.build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator by parsing configuration from a URI.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// # fn example() -> Result<()> {
    /// let op = Operator::from_uri("memory://localhost/")?;
    /// # let _ = op;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_uri(uri: impl IntoOperatorUri) -> Result<Operator> {
        crate::DEFAULT_OPERATOR_REGISTRY.load(uri)
    }

    /// Create a new operator via given scheme and iterator of config value in dynamic dispatch.
    ///
    /// # Notes
    ///
    /// `via_iter` generates a `Operator` which allows building operator without generic type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// use std::collections::HashMap;
    ///
    /// use opendal::Operator;
    /// use opendal::services;
    ///
    /// async fn test() -> Result<()> {
    ///     let map = [
    ///         // Set the root for fs, all operations will happen under this root.
    ///         //
    ///         // NOTE: the root must be absolute path.
    ///         ("root".to_string(), "/tmp".to_string()),
    ///     ];
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::via_iter(services::FS_SCHEME, map)?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[allow(unused_variables, unreachable_code)]
    pub fn via_iter(
        scheme: impl AsRef<str>,
        iter: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        let op = match scheme.as_ref() {
            #[cfg(feature = "services-aliyun-drive")]
            services::ALIYUN_DRIVE_SCHEME => {
                Self::from_iter::<services::AliyunDrive>(iter)?.finish()
            }
            #[cfg(feature = "services-alluxio")]
            services::ALLUXIO_SCHEME => Self::from_iter::<services::Alluxio>(iter)?.finish(),
            #[cfg(feature = "services-azblob")]
            services::AZBLOB_SCHEME => Self::from_iter::<services::Azblob>(iter)?.finish(),
            #[cfg(feature = "services-azdls")]
            services::AZDLS_SCHEME => Self::from_iter::<services::Azdls>(iter)?.finish(),
            #[cfg(feature = "services-azfile")]
            services::AZFILE_SCHEME => Self::from_iter::<services::Azfile>(iter)?.finish(),
            #[cfg(feature = "services-b2")]
            services::B2_SCHEME => Self::from_iter::<services::B2>(iter)?.finish(),
            #[cfg(feature = "services-cacache")]
            services::CACACHE_SCHEME => Self::from_iter::<services::Cacache>(iter)?.finish(),
            #[cfg(feature = "services-cloudflare-kv")]
            services::CLOUDFLARE_KV_SCHEME => {
                Self::from_iter::<services::CloudflareKv>(iter)?.finish()
            }
            #[cfg(feature = "services-compfs")]
            services::COMPFS_SCHEME => Self::from_iter::<services::Compfs>(iter)?.finish(),
            #[cfg(feature = "services-cos")]
            services::COS_SCHEME => Self::from_iter::<services::Cos>(iter)?.finish(),
            #[cfg(feature = "services-d1")]
            services::D1_SCHEME => Self::from_iter::<services::D1>(iter)?.finish(),
            #[cfg(feature = "services-dashmap")]
            services::DASHMAP_SCHEME => Self::from_iter::<services::Dashmap>(iter)?.finish(),
            #[cfg(feature = "services-dbfs")]
            services::DBFS_SCHEME => Self::from_iter::<services::Dbfs>(iter)?.finish(),
            #[cfg(feature = "services-dropbox")]
            services::DROPBOX_SCHEME => Self::from_iter::<services::Dropbox>(iter)?.finish(),
            #[cfg(feature = "services-etcd")]
            services::ETCD_SCHEME => Self::from_iter::<services::Etcd>(iter)?.finish(),
            #[cfg(feature = "services-foundationdb")]
            services::FOUNDATIONDB_SCHEME => {
                Self::from_iter::<services::Foundationdb>(iter)?.finish()
            }
            #[cfg(feature = "services-fs")]
            services::FS_SCHEME => Self::from_iter::<services::Fs>(iter)?.finish(),
            #[cfg(feature = "services-ftp")]
            services::FTP_SCHEME => Self::from_iter::<services::Ftp>(iter)?.finish(),
            #[cfg(feature = "services-gcs")]
            services::GCS_SCHEME => Self::from_iter::<services::Gcs>(iter)?.finish(),
            #[cfg(feature = "services-gdrive")]
            services::GDRIVE_SCHEME => Self::from_iter::<services::Gdrive>(iter)?.finish(),
            #[cfg(feature = "services-ghac")]
            services::GHAC_SCHEME => Self::from_iter::<services::Ghac>(iter)?.finish(),
            #[cfg(feature = "services-github")]
            services::GITHUB_SCHEME => Self::from_iter::<services::Github>(iter)?.finish(),
            #[cfg(feature = "services-gridfs")]
            services::GRIDFS_SCHEME => Self::from_iter::<services::Gridfs>(iter)?.finish(),
            #[cfg(feature = "services-hdfs")]
            services::HDFS_SCHEME => Self::from_iter::<services::Hdfs>(iter)?.finish(),
            #[cfg(feature = "services-hdfs-native")]
            services::HDFS_NATIVE_SCHEME => Self::from_iter::<services::HdfsNative>(iter)?.finish(),
            #[cfg(feature = "services-http")]
            services::HTTP_SCHEME => Self::from_iter::<services::Http>(iter)?.finish(),
            #[cfg(feature = "services-huggingface")]
            services::HUGGINGFACE_SCHEME => {
                Self::from_iter::<services::Huggingface>(iter)?.finish()
            }
            #[cfg(feature = "services-ipfs")]
            services::IPFS_SCHEME => Self::from_iter::<services::Ipfs>(iter)?.finish(),
            #[cfg(feature = "services-ipmfs")]
            services::IPMFS_SCHEME => Self::from_iter::<services::Ipmfs>(iter)?.finish(),
            #[cfg(feature = "services-koofr")]
            services::KOOFR_SCHEME => Self::from_iter::<services::Koofr>(iter)?.finish(),
            #[cfg(feature = "services-lakefs")]
            services::LAKEFS_SCHEME => Self::from_iter::<services::Lakefs>(iter)?.finish(),
            #[cfg(feature = "services-memcached")]
            services::MEMCACHED_SCHEME => Self::from_iter::<services::Memcached>(iter)?.finish(),
            #[cfg(feature = "services-memory")]
            services::MEMORY_SCHEME => Self::from_iter::<services::Memory>(iter)?.finish(),
            #[cfg(feature = "services-mini-moka")]
            services::MINI_MOKA_SCHEME => Self::from_iter::<services::MiniMoka>(iter)?.finish(),
            #[cfg(feature = "services-moka")]
            services::MOKA_SCHEME => Self::from_iter::<services::Moka>(iter)?.finish(),
            #[cfg(feature = "services-mongodb")]
            services::MONGODB_SCHEME => Self::from_iter::<services::Mongodb>(iter)?.finish(),
            #[cfg(feature = "services-monoiofs")]
            services::MONOIOFS_SCHEME => Self::from_iter::<services::Monoiofs>(iter)?.finish(),
            #[cfg(feature = "services-mysql")]
            services::MYSQL_SCHEME => Self::from_iter::<services::Mysql>(iter)?.finish(),
            #[cfg(feature = "services-obs")]
            services::OBS_SCHEME => Self::from_iter::<services::Obs>(iter)?.finish(),
            #[cfg(feature = "services-onedrive")]
            services::ONEDRIVE_SCHEME => Self::from_iter::<services::Onedrive>(iter)?.finish(),
            #[cfg(feature = "services-oss")]
            services::OSS_SCHEME => Self::from_iter::<services::Oss>(iter)?.finish(),
            #[cfg(feature = "services-pcloud")]
            services::PCLOUD_SCHEME => Self::from_iter::<services::Pcloud>(iter)?.finish(),
            #[cfg(feature = "services-persy")]
            services::PERSY_SCHEME => Self::from_iter::<services::Persy>(iter)?.finish(),
            #[cfg(feature = "services-postgresql")]
            services::POSTGRESQL_SCHEME => Self::from_iter::<services::Postgresql>(iter)?.finish(),
            #[cfg(feature = "services-redb")]
            services::REDB_SCHEME => Self::from_iter::<services::Redb>(iter)?.finish(),
            #[cfg(feature = "services-redis")]
            services::REDIS_SCHEME => Self::from_iter::<services::Redis>(iter)?.finish(),
            #[cfg(feature = "services-rocksdb")]
            services::ROCKSDB_SCHEME => Self::from_iter::<services::Rocksdb>(iter)?.finish(),
            #[cfg(feature = "services-s3")]
            services::S3_SCHEME => Self::from_iter::<services::S3>(iter)?.finish(),
            #[cfg(feature = "services-seafile")]
            services::SEAFILE_SCHEME => Self::from_iter::<services::Seafile>(iter)?.finish(),
            #[cfg(feature = "services-sftp")]
            services::SFTP_SCHEME => Self::from_iter::<services::Sftp>(iter)?.finish(),
            #[cfg(feature = "services-sled")]
            services::SLED_SCHEME => Self::from_iter::<services::Sled>(iter)?.finish(),
            #[cfg(feature = "services-sqlite")]
            services::SQLITE_SCHEME => Self::from_iter::<services::Sqlite>(iter)?.finish(),
            #[cfg(feature = "services-surrealdb")]
            services::SURREALDB_SCHEME => Self::from_iter::<services::Surrealdb>(iter)?.finish(),
            #[cfg(feature = "services-swift")]
            services::SWIFT_SCHEME => Self::from_iter::<services::Swift>(iter)?.finish(),
            #[cfg(feature = "services-tikv")]
            services::TIKV_SCHEME => Self::from_iter::<services::Tikv>(iter)?.finish(),
            #[cfg(feature = "services-upyun")]
            services::UPYUN_SCHEME => Self::from_iter::<services::Upyun>(iter)?.finish(),
            #[cfg(feature = "services-vercel-artifacts")]
            services::VERCEL_ARTIFACTS_SCHEME => {
                Self::from_iter::<services::VercelArtifacts>(iter)?.finish()
            }
            #[cfg(feature = "services-vercel-blob")]
            services::VERCEL_BLOB_SCHEME => Self::from_iter::<services::VercelBlob>(iter)?.finish(),
            #[cfg(feature = "services-webdav")]
            services::WEBDAV_SCHEME => Self::from_iter::<services::Webdav>(iter)?.finish(),
            #[cfg(feature = "services-webhdfs")]
            services::WEBHDFS_SCHEME => Self::from_iter::<services::Webhdfs>(iter)?.finish(),
            #[cfg(feature = "services-yandex-disk")]
            services::YANDEX_DISK_SCHEME => Self::from_iter::<services::YandexDisk>(iter)?.finish(),
            v => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "scheme is not enabled or supported",
                )
                .with_context("scheme", v));
            }
        };

        Ok(op)
    }

    /// Create a new layer with dynamic dispatch.
    ///
    /// Please note that `Layer` can modify internal contexts such as `HttpClient`
    /// and `Runtime` for the operator. Therefore, it is recommended to add layers
    /// before interacting with the storage. Adding or duplicating layers after
    /// accessing the storage may result in unexpected behavior.
    ///
    /// # Notes
    ///
    /// `OperatorBuilder::layer()` is using static dispatch which is zero
    /// cost. `Operator::layer()` is using dynamic dispatch which has a
    /// bit runtime overhead with an extra vtable lookup and unable to
    /// inline.
    ///
    /// It's always recommended to use `OperatorBuilder::layer()` instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// use opendal::layers::LoggingLayer;
    /// use opendal::services::Memory;
    /// use opendal::Operator;
    ///
    /// # async fn test() -> Result<()> {
    /// let op = Operator::new(Memory::default())?.finish();
    /// let op = op.layer(LoggingLayer::default());
    /// // All operations will go through the new_layer
    /// let _ = op.read("test_file").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn layer<L: Layer<Accessor>>(self, layer: L) -> Self {
        Self::from_inner(Arc::new(
            TypeEraseLayer.layer(layer.layer(self.into_inner())),
        ))
    }
}

/// OperatorBuilder is a typed builder to build an Operator.
///
/// # Notes
///
/// OpenDAL uses static dispatch internally and only performs dynamic
/// dispatch at the outmost type erase layer. OperatorBuilder is the only
/// public API provided by OpenDAL come with generic parameters.
///
/// It's required to call `finish` after the operator built.
///
/// # Examples
///
/// For users who want to support many services, we can build a helper function like the following:
///
/// ```
/// use std::collections::HashMap;
///
/// use opendal::layers::LoggingLayer;
/// use opendal::layers::RetryLayer;
/// use opendal::services;
/// use opendal::Builder;
/// use opendal::Operator;
/// use opendal::Result;
///
/// fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator> {
///     let op = Operator::from_iter::<B>(cfg)?
///         .layer(LoggingLayer::default())
///         .layer(RetryLayer::new())
///         .finish();
///
///     Ok(op)
/// }
///
/// async fn init(scheme: &str, cfg: HashMap<String, String>) -> Result<()> {
///     let _ = match scheme {
///         services::MEMORY_SCHEME => init_service::<services::Memory>(cfg)?,
///         _ => todo!(),
///     };
///
///     Ok(())
/// }
/// ```
pub struct OperatorBuilder<A: Access> {
    accessor: A,
}

impl<A: Access> OperatorBuilder<A> {
    /// Create a new operator builder.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(accessor: A) -> OperatorBuilder<impl Access> {
        // Make sure error context layer has been attached.
        OperatorBuilder { accessor }
            .layer(ErrorContextLayer)
            .layer(CompleteLayer)
            .layer(CorrectnessCheckLayer)
    }

    /// Create a new layer with static dispatch.
    ///
    /// # Notes
    ///
    /// `OperatorBuilder::layer()` is using static dispatch which is zero
    /// cost. `Operator::layer()` is using dynamic dispatch which has a
    /// bit runtime overhead with an extra vtable lookup and unable to
    /// inline.
    ///
    /// It's always recommended to use `OperatorBuilder::layer()` instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// use opendal::layers::LoggingLayer;
    /// use opendal::services::Memory;
    /// use opendal::Operator;
    ///
    /// # async fn test() -> Result<()> {
    /// let op = Operator::new(Memory::default())?
    ///     .layer(LoggingLayer::default())
    ///     .finish();
    /// // All operations will go through the new_layer
    /// let _ = op.read("test_file").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn layer<L: Layer<A>>(self, layer: L) -> OperatorBuilder<L::LayeredAccess> {
        OperatorBuilder {
            accessor: layer.layer(self.accessor),
        }
    }

    /// Finish the building to construct an Operator.
    pub fn finish(self) -> Operator {
        let ob = self.layer(TypeEraseLayer);
        Operator::from_inner(Arc::new(ob.accessor) as Accessor)
    }
}
