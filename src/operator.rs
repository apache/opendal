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

use std::env;
use std::sync::Arc;

use futures::StreamExt;
use futures::TryStreamExt;

use crate::object::ObjectLister;
use crate::raw::*;
use crate::services;
use crate::Error;
use crate::ErrorKind;
use crate::Layer;
use crate::Object;
use crate::ObjectMode;
use crate::Result;
use crate::Scheme;

/// User-facing APIs for object and object streams.
#[derive(Clone, Debug)]
pub struct Operator {
    accessor: Arc<dyn Accessor>,
}

impl<A> From<A> for Operator
where
    A: Accessor,
{
    fn from(accessor: A) -> Self {
        Operator::new(accessor)
    }
}

impl From<Arc<dyn Accessor>> for Operator {
    fn from(accessor: Arc<dyn Accessor>) -> Self {
        Operator { accessor }
    }
}

impl Operator {
    /// Create a new operator.
    ///
    /// # Examples
    ///
    /// Read more backend init examples in [examples](https://github.com/datafuselabs/opendal/tree/main/examples).
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// # use opendal::services::fs;
    /// # use opendal::Object;
    /// # use opendal::Operator;
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // Create fs backend builder.
    ///     let mut builder = fs::Builder::default();
    ///     // Set the root for fs, all operations will happen under this root.
    ///     //
    ///     // NOTE: the root must be absolute path.
    ///     builder.root("/tmp");
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::new(builder.build()?);
    ///
    ///     // Create an object handle to start operation on object.
    ///     let _: Object = op.object("test_file");
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn new(accessor: impl Accessor) -> Self {
        Self {
            accessor: Arc::new(accessor),
        }
    }

    /// Create a new operator from iter.
    ///
    /// Please refer different backends for detailed config options.
    ///
    /// # Behavior
    ///
    /// - All input key must be `lower_case`
    /// - Boolean values will be checked by its existences and non-empty value.
    ///   `on`, `yes`, `true`, `off`, `no`, `false` will all be treated as `true`
    ///   To disable a flag, please set value to empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// # use opendal::services::fs;
    /// # use opendal::Object;
    /// # use opendal::Operator;
    /// # use opendal::Scheme;
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let op: Operator = Operator::from_iter(
    ///         Scheme::Fs,
    ///         [("root".to_string(), "/tmp".to_string())].into_iter(),
    ///     )?;
    ///
    ///     // Create an object handle to start operation on object.
    ///     let _: Object = op.object("test_file");
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn from_iter(
        scheme: Scheme,
        it: impl Iterator<Item = (String, String)> + 'static,
    ) -> Result<Self> {
        let op = match scheme {
            Scheme::Azblob => services::azblob::Builder::from_iter(it).build()?.into(),
            Scheme::Azdfs => services::azdfs::Builder::from_iter(it).build()?.into(),
            Scheme::WebHdfs => services::webhdfs::Builder::from_iter(it).build()?.into(),
            Scheme::Fs => services::fs::Builder::from_iter(it).build()?.into(),
            #[cfg(feature = "services-ftp")]
            Scheme::Ftp => services::ftp::Builder::from_iter(it).build()?.into(),
            Scheme::Gcs => services::gcs::Builder::from_iter(it).build()?.into(),
            Scheme::Ghac => services::ghac::Builder::from_iter(it).build()?.into(),
            #[cfg(feature = "services-hdfs")]
            Scheme::Hdfs => services::hdfs::Builder::from_iter(it).build()?.into(),
            Scheme::Http => services::http::Builder::from_iter(it).build()?.into(),
            #[cfg(feature = "services-ipfs")]
            Scheme::Ipfs => services::ipfs::Builder::from_iter(it).build()?.into(),
            Scheme::Ipmfs => services::ipmfs::Builder::from_iter(it).build()?.into(),
            #[cfg(feature = "services-memcached")]
            Scheme::Memcached => services::memcached::Builder::from_iter(it).build()?.into(),
            Scheme::Memory => services::memory::Builder::default().build()?.into(),
            #[cfg(feature = "services-moka")]
            Scheme::Moka => services::moka::Builder::from_iter(it).build()?.into(),
            Scheme::Obs => services::obs::Builder::from_iter(it).build()?.into(),
            Scheme::Oss => services::oss::Builder::from_iter(it).build()?.into(),
            #[cfg(feature = "services-redis")]
            Scheme::Redis => services::redis::Builder::from_iter(it).build()?.into(),
            #[cfg(feature = "services-rocksdb")]
            Scheme::Rocksdb => services::rocksdb::Builder::from_iter(it).build()?.into(),
            Scheme::S3 => services::s3::Builder::from_iter(it).build()?.into(),
            Scheme::Webdav => services::webdav::Builder::from_iter(it).build()?.into(),
            Scheme::Custom(v) => {
                return Err(
                    Error::new(ErrorKind::Unsupported, "custom service  is not supported")
                        .with_context("service", v),
                )
            }
        };

        Ok(op)
    }

    /// Create a new operator from env.
    ///
    /// # Behavior
    ///
    /// - Environment keys are case-insensitive, they will be converted to lower case internally.
    /// - Environment values are case-sensitive, no sanity will be executed on them.
    /// - Boolean values will be checked by its existences and non-empty value.
    ///   `on`, `yes`, `true`, `off`, `no`, `false` will all be treated as `true`
    ///   To disable a flag, please set value to empty.
    ///
    /// # Examples
    ///
    /// Setting environment:
    ///
    /// ```shell
    /// export OPENDAL_FS_ROOT=/tmp
    /// ```
    ///
    /// Please refer different backends for detailed config options.
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use opendal::Object;
    /// # use opendal::Operator;
    /// # use opendal::Scheme;
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // Build an Operator to start operating the storage
    ///     let op: Operator = Operator::from_env(Scheme::Fs)?;
    ///
    ///     // Create an object handle to start operation on object.
    ///     let _: Object = op.object("test_file");
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn from_env(scheme: Scheme) -> Result<Self> {
        let prefix = format!("opendal_{scheme}_");
        let envs = env::vars().filter_map(move |(k, v)| {
            k.to_lowercase()
                .strip_prefix(&prefix)
                .map(|k| (k.to_string(), v))
        });

        Self::from_iter(scheme, envs)
    }

    /// Create a new layer.
    ///
    /// # Examples
    ///
    /// This examples needs feature `retry` enabled.
    ///
    /// ```no_build
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// # use opendal::services::fs;
    /// # use opendal::services::fs::Builder;
    /// use opendal::Operator;
    /// use opendal::Layer;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let accessor = fs::Backend::build().finish().await?;
    /// let op = Operator::new(accessor).layer(new_layer);
    /// // All operations will go through the new_layer
    /// let _ = op.object("test_file").read();
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn layer(self, layer: impl Layer) -> Self {
        Operator {
            accessor: layer.layer(self.accessor.clone()),
        }
    }

    /// Get inner accessor.
    ///
    /// This function should only be used by developers to implement layers.
    pub fn inner(&self) -> Arc<dyn Accessor> {
        self.accessor.clone()
    }

    /// Get metadata of underlying accessor.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// # use opendal::services::fs;
    /// # use opendal::services::fs::Builder;
    /// use opendal::Operator;
    /// use opendal::Scheme;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let op = Operator::from_env(Scheme::Fs)?;
    /// let meta = op.metadata();
    /// # Ok(())
    /// # }
    /// ```
    pub fn metadata(&self) -> OperatorMetadata {
        OperatorMetadata {
            acc: self.accessor.metadata(),
        }
    }

    /// Create a new batch operator handle to take batch operations
    /// like `walk` and `remove`.
    pub fn batch(&self) -> BatchOperator {
        BatchOperator::new(self.clone())
    }

    /// Create a new [`Object`][crate::Object] handle to take operations.
    pub fn object(&self, path: &str) -> Object {
        Object::new(self.clone(), path)
    }

    /// Check if this operator can work correctly.
    ///
    /// We will send a `list` request to path and return any errors we met.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// # use opendal::services::fs;
    /// # use opendal::services::fs::Builder;
    /// use opendal::Operator;
    /// use opendal::Scheme;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let op = Operator::from_env(Scheme::Fs)?;
    /// op.check().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check(&self) -> Result<()> {
        let mut ds = self.object("/").list().await?;

        match ds.next().await {
            Some(Err(e)) if e.kind() != ErrorKind::ObjectNotFound => Err(e),
            _ => Ok(()),
        }
    }
}

/// BatchOperator is used to take batch operations like walk_dir and remove_all, should
/// be constructed by [`Operator::batch()`].
///
/// # TODO
///
/// We will support batch operators between two different operators like copy and move.
#[derive(Clone, Debug)]
pub struct BatchOperator {
    src: Operator,
}

impl BatchOperator {
    pub(crate) fn new(op: Operator) -> Self {
        BatchOperator { src: op }
    }

    /// Walk a dir in the best way that suitable for underlying storage.
    ///
    /// The returning order could be differ for different underlying storage.
    /// And could be changed at any time. Users MUST NOT relay on the order.
    pub fn walk(&self, path: &str) -> Result<ObjectLister> {
        // # TODO
        //
        // After https://github.com/datafuselabs/opendal/issues/353, we can
        // use prefix list for walk_bottom_up.
        self.walk_top_down(path)
    }

    /// Walk a dir in top down way: list current dir first and then list nested dir.
    ///
    /// Refer to [`TopDownWalker`] for more about the behavior details.
    pub fn walk_top_down(&self, path: &str) -> Result<ObjectLister> {
        Ok(ObjectLister::new(
            self.src.clone(),
            Box::new(TopDownWalker::new(self.src.inner(), path)),
        ))
    }

    /// Walk a dir in bottom up way: list nested dir first and then current dir.
    ///
    /// Refer to [`BottomUpWalker`] for more about the behavior details.
    pub fn walk_bottom_up(&self, path: &str) -> Result<ObjectLister> {
        Ok(ObjectLister::new(
            self.src.clone(),
            Box::new(BottomUpWalker::new(self.src.inner(), path)),
        ))
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// **Use this function in cautions to avoid unexpected data loss.**
    pub async fn remove_all(&self, path: &str) -> Result<()> {
        let parent = self.src.object(path);
        let meta = parent.metadata().await?;

        if meta.mode() != ObjectMode::DIR {
            return parent.delete().await;
        }

        let obs = self.walk_bottom_up(path)?;
        obs.try_for_each(|v| async move { v.delete().await }).await
    }
}

/// Metadata for operator, users can use this metadata to get information of operator.
#[derive(Clone, Debug, Default)]
pub struct OperatorMetadata {
    acc: AccessorMetadata,
}

impl OperatorMetadata {
    /// [`Scheme`] of operator.
    pub fn scheme(&self) -> Scheme {
        self.acc.scheme()
    }

    /// Root of operator, will be in format like `/path/to/dir/`
    pub fn root(&self) -> &str {
        self.acc.root()
    }

    /// Name of backend, could be empty if underlying backend doesn't have namespace concept.
    ///
    /// For example:
    ///
    /// - name for `s3` => bucket name
    /// - name for `azblob` => container name
    pub fn name(&self) -> &str {
        self.acc.name()
    }

    /// Check if current backend supports [`Accessor::read`] or not.
    pub fn can_read(&self) -> bool {
        self.acc.capabilities().contains(AccessorCapability::Read)
    }

    /// Check if current backend supports [`Accessor::write`] or not.
    pub fn can_write(&self) -> bool {
        self.acc.capabilities().contains(AccessorCapability::Write)
    }

    /// Check if current backend supports [`Accessor::list`] or not.
    pub fn can_list(&self) -> bool {
        self.acc.capabilities().contains(AccessorCapability::List)
    }

    /// Check if current backend supports [`Accessor::presign`] or not.
    pub fn can_presign(&self) -> bool {
        self.acc
            .capabilities()
            .contains(AccessorCapability::Presign)
    }

    /// Check if current backend supports multipart operations or not.
    pub fn can_multipart(&self) -> bool {
        self.acc
            .capabilities()
            .contains(AccessorCapability::Multipart)
    }

    /// Check if current backend supports blocking operations or not.
    pub fn can_blocking(&self) -> bool {
        self.acc
            .capabilities()
            .contains(AccessorCapability::Blocking)
    }
}
