// Copyright 2022 Datafuse Labs
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
use std::sync::Arc;

use futures::stream;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::layers::*;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Operator is the user-facing APIs for object and object streams.
///
/// Operator needs to be built with [`Builder`].
#[derive(Clone, Debug)]
pub struct Operator {
    accessor: FusedAccessor,
}

impl From<FusedAccessor> for Operator {
    fn from(accessor: FusedAccessor) -> Self {
        Self { accessor }
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
    /// # use anyhow::Result;
    /// use opendal::services::Fs;
    /// use opendal::Builder;
    /// use opendal::Operator;
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // Create fs backend builder.
    ///     let mut builder = Fs::default();
    ///     // Set the root for fs, all operations will happen under this root.
    ///     //
    ///     // NOTE: the root must be absolute path.
    ///     builder.root("/tmp");
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::new(builder.build()?).finish();
    ///
    ///     // Create an object handle to start operation on object.
    ///     let _ = op.object("test_file");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[allow(clippy::new_ret_no_self)]
    pub fn new<A: Accessor>(acc: A) -> OperatorBuilder<impl Accessor> {
        OperatorBuilder::new(acc)
    }

    /// Create a new operator with input builder.
    ///
    /// OpenDAL will call `builder.build()` internally, so we don't need
    /// to import `opendal::Builder` trait.
    ///
    /// # Examples
    ///
    /// Read more backend init examples in [examples](https://github.com/datafuselabs/opendal/tree/main/examples).
    ///
    /// ```
    /// # use anyhow::Result;
    /// use opendal::services::Fs;
    /// use opendal::Operator;
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     // Create fs backend builder.
    ///     let mut builder = Fs::default();
    ///     // Set the root for fs, all operations will happen under this root.
    ///     //
    ///     // NOTE: the root must be absolute path.
    ///     builder.root("/tmp");
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::create(builder)?.finish();
    ///
    ///     // Create an object handle to start operation on object.
    ///     let _ = op.object("test_file");
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn create<B: Builder>(mut ab: B) -> Result<OperatorBuilder<impl Accessor>> {
        let acc = ab.build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator from given map.
    ///
    /// ```
    /// # use anyhow::Result;
    /// use std::collections::HashMap;
    ///
    /// use opendal::services::Fs;
    /// use opendal::Operator;
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let map = HashMap::from([
    ///         // Set the root for fs, all operations will happen under this root.
    ///         //
    ///         // NOTE: the root must be absolute path.
    ///         ("root".to_string(), "/tmp".to_string()),
    ///     ]);
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::from_map::<Fs>(map)?.finish();
    ///
    ///     // Create an object handle to start operation on object.
    ///     let _ = op.object("test_file");
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn from_map<B: Builder>(
        map: HashMap<String, String>,
    ) -> Result<OperatorBuilder<impl Accessor>> {
        let acc = B::from_map(map).build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator from iter.
    ///
    /// # WARNING
    ///
    /// It's better to use `from_map`. We may remove this API in the
    /// future.
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<B: Builder>(
        iter: impl Iterator<Item = (String, String)>,
    ) -> Result<OperatorBuilder<impl Accessor>> {
        let acc = B::from_iter(iter).build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator from env.
    ///
    /// # WARNING
    ///
    /// It's better to use `from_map`. We may remove this API in the
    /// future.
    pub fn from_env<B: Builder>() -> Result<OperatorBuilder<impl Accessor>> {
        let acc = B::from_env().build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Get inner accessor.
    ///
    /// This function should only be used by developers to implement layers.
    pub fn inner(&self) -> FusedAccessor {
        self.accessor.clone()
    }

    /// Create a new layer with dynamic dispatch.
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
    /// use opendal::services::Fs;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let op = Operator::create(Fs::default())?.finish();
    /// let op = op.layer(LoggingLayer::default());
    /// // All operations will go through the new_layer
    /// let _ = op.object("test_file").read().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn layer<L: Layer<FusedAccessor>>(self, layer: L) -> Self {
        Self {
            accessor: Arc::new(TypeEraseLayer.layer(layer.layer(self.accessor))),
        }
    }

    /// Get metadata of underlying accessor.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
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
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
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

/// OperatorBuilder is a typed builder to builder an Operator.
///
/// # Notes
///
/// OpenDAL uses static dispatch internally and only perform dynamic
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
/// use opendal::Scheme;
///
/// fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator> {
///     let op = Operator::from_map::<B>(cfg)?
///         .layer(LoggingLayer::default())
///         .layer(RetryLayer::new())
///         .finish();
///
///     Ok(op)
/// }
///
/// async fn init(scheme: Scheme, cfg: HashMap<String, String>) -> Result<()> {
///     let _ = match scheme {
///         Scheme::S3 => init_service::<services::S3>(cfg)?,
///         Scheme::Fs => init_service::<services::Fs>(cfg)?,
///         _ => todo!(),
///     };
///
///     Ok(())
/// }
/// ```
pub struct OperatorBuilder<A: Accessor> {
    accessor: A,
}

impl<A: Accessor> OperatorBuilder<A> {
    /// Create a new operator builder.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(accessor: A) -> OperatorBuilder<impl Accessor> {
        // Make sure error context layer has been attached.
        OperatorBuilder { accessor }
            .layer(ErrorContextLayer)
            .layer(CompleteLayer)
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
    /// use opendal::services::Fs;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let op = Operator::create(Fs::default())?
    ///     .layer(LoggingLayer::default())
    ///     .finish();
    /// // All operations will go through the new_layer
    /// let _ = op.object("test_file").read().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn layer<L: Layer<A>>(self, layer: L) -> OperatorBuilder<L::LayeredAccessor> {
        OperatorBuilder {
            accessor: layer.layer(self.accessor),
        }
    }

    /// Finish the building to construct an Operator.
    pub fn finish(self) -> Operator {
        let ob = self.layer(TypeEraseLayer);

        Operator {
            accessor: Arc::new(ob.accessor),
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
    meta: OperatorMetadata,

    limit: usize,
}

impl BatchOperator {
    pub(crate) fn new(op: Operator) -> Self {
        let meta = op.metadata();

        BatchOperator {
            src: op,
            meta,
            limit: 1000,
        }
    }

    /// Specify the batch limit.
    ///
    /// Default: 1000
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// remove will given paths.
    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.batch()
    ///     .remove(vec!["abc".to_string(), "def".to_string()])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove(&self, paths: Vec<String>) -> Result<()> {
        self.remove_via(stream::iter(paths)).await
    }

    /// remove_via will remove objects via given stream.
    ///
    /// We will delete by chunks with given batch limit on the stream.
    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// use futures::stream;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let stream = stream::iter(vec!["abc".to_string(), "def".to_string()]);
    /// op.batch().remove_via(stream).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_via(&self, mut input: impl Stream<Item = String> + Unpin) -> Result<()> {
        if self.meta.can_batch() {
            let mut input = input.map(|v| (v, OpDelete::default())).chunks(self.limit);

            while let Some(batches) = input.next().await {
                let results = self
                    .src
                    .accessor
                    .batch(OpBatch::new(BatchOperations::Delete(batches)))
                    .await?;

                let BatchedResults::Delete(results) = results.into_results();

                // TODO: return error here directly seems not a good idea?
                for (_, result) in results {
                    let _ = result?;
                }
            }
        } else {
            while let Some(path) = input.next().await {
                self.src.accessor.delete(&path, OpDelete::default()).await?;
            }
        }

        Ok(())
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.batch().remove_all("path/to/dir").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_all(&self, path: &str) -> Result<()> {
        let parent = self.src.object(path);
        let meta = parent.stat().await?;

        if meta.mode() != ObjectMode::DIR {
            return parent.delete().await;
        }

        let obs = parent.scan().await?;

        if self.meta.can_batch() {
            let mut obs = obs.try_chunks(self.limit);

            while let Some(batches) = obs.next().await {
                let batches = batches
                    .map_err(|err| err.1)?
                    .into_iter()
                    .map(|v| (v.path().to_string(), OpDelete::default()))
                    .collect();

                let results = self
                    .src
                    .accessor
                    .batch(OpBatch::new(BatchOperations::Delete(batches)))
                    .await?;

                let BatchedResults::Delete(results) = results.into_results();

                // TODO: return error here directly seems not a good idea?
                for (_, result) in results {
                    let _ = result?;
                }
            }
        } else {
            obs.try_for_each(|v| async move { v.delete().await })
                .await?;
        }

        Ok(())
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

    /// Check if current backend supports [`Accessor::scan`] or not.
    pub fn can_scan(&self) -> bool {
        self.acc.capabilities().contains(AccessorCapability::Scan)
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

    /// Check if current backend supports batch operations or not.
    pub fn can_batch(&self) -> bool {
        self.acc.capabilities().contains(AccessorCapability::Batch)
    }

    /// Check if current backend supports blocking operations or not.
    pub fn can_blocking(&self) -> bool {
        self.acc
            .capabilities()
            .contains(AccessorCapability::Blocking)
    }
}
